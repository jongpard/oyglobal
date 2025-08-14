import asyncio
import json
import os
import re
import time
from typing import List, Dict, Any, Optional

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_fixed

from price_parser import parse_price

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# 다중 셀렉터 후보 (구조 변경 대응)
PRODUCT_CARD_SELECTORS = [
    'li[data-product-id]',            # 데이터 속성이 있는 카드
    'ul[class*="prd"] li',            # prd 리스트
    'ul[class*="list"] li',
    'div[class*="product"] li',
    'li[class*="item"]',
]
NAME_SELECTORS = [
    '.prod-name', '.name', '.tit', 'a[title]', 'img[alt]'
]
BRAND_SELECTORS = [
    '.brand', '.prod-brand', '.brand-name'
]
PRICE_WRAP_SELECTORS = [
    '.price', '.prod-price', '.price-area', '.cost', '.amount'
]
LINK_SELECTORS = [
    'a[href*="/product/"]', 'a[href*="/goods/"]', 'a[href]'
]

DEBUG_DIR = "data/debug"

def _ensure_debug_dirs():
    if not os.path.exists(DEBUG_DIR):
        os.makedirs(DEBUG_DIR, exist_ok=True)

async def _click_if_exists(page, selectors_or_text: List[str]) -> bool:
    for sel in selectors_or_text:
        try:
            if sel.startswith("text="):
                locator = page.get_by_text(sel.replace("text=", ""), exact=False)
                if await locator.count() > 0:
                    await locator.first.click()
                    return True
            else:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click()
                    return True
        except Exception:
            continue
    return False

async def _force_region_us(page) -> None:
    """
    미국 기준 노출을 위해 가능한 모든 방법을 시도
    """
    # 쿠키/모달/드롭다운 대응 (문구/셀렉터 다중 시도)
    candidates = [
        'button[aria-label*="Ship"]',
        'button:has-text("Ship to")',
        'a:has-text("Ship to")',
        'button:has-text("United States")',
        'a:has-text("United States")',
        'text=Ship to',
        'text=United States',
        'text=미국',
        'text=배송지',
        'text=Country',
    ]
    await _click_if_exists(page, candidates)
    # "United States" 선택
    await _click_if_exists(page, [
        'li:has-text("United States")',
        'button:has-text("United States")',
        'text=United States'
    ])
    # 통화/언어 설정 같은 모달 닫기
    await _click_if_exists(page, ['button:has-text("Save")', 'button:has-text("Apply")', 'button:has-text("OK")'])

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def _wait_network_settle(page, timeout_ms=8000):
    await page.wait_for_load_state("networkidle", timeout=timeout_ms)

async def _scroll_to_load(page, target_count=100, step_px=2000, max_rounds=20):
    last_height = 0
    for i in range(max_rounds):
        await page.evaluate(f"window.scrollBy(0, {step_px});")
        await asyncio.sleep(0.7)
        await _wait_network_settle(page, 8000)
        height = await page.evaluate("document.body.scrollHeight")
        if height == last_height:
            break
        last_height = height
        # 충분히 로드되었는지 카드 개수를 확인
        cards_count = 0
        for sel in PRODUCT_CARD_SELECTORS:
            cards_count = max(cards_count, await page.locator(sel).count())
        if cards_count >= target_count:
            break

def _extract_text(el, selectors: List[str]) -> str:
    for sel in selectors:
        target = el.select_one(sel)
        if target:
            txt = target.get_text(strip=True)
            if not txt and target.has_attr("alt"):
                txt = target["alt"].strip()
            if txt:
                return txt
    return ""

def _extract_link(el, selectors: List[str]) -> str:
    for sel in selectors:
        target = el.select_one(sel)
        if target and target.has_attr("href"):
            href = target["href"]
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://global.oliveyoung.com" + href
            return href
    return ""

def _extract_prices(el) -> (Optional[float], Optional[float]):
    # sale_price, original_price
    text = el.get_text(" ", strip=True)
    # $12.34 $15.00, 15.00 → 여러 형태 대응
    sale = parse_price(text)
    # strike-through나 'original' 클래스 찾기(있다면)
    strike = el.select_one('del, .origin, .original, .strike, .price-origin')
    original = parse_price(strike.get_text(" ", strip=True)) if strike else None
    # 뒤집혔을 가능성 처리
    if original and sale and original < sale:
        sale, original = original, sale
    return sale, original

def _calc_discount(sale: Optional[float], original: Optional[float]) -> Optional[int]:
    if sale and original and original > 0 and sale <= original:
        pct = round((original - sale) / original * 100)
        return int(pct)
    return None

def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    # URL 또는 (브랜드+제품명) 기준으로 중복 제거
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")
    df = df.drop_duplicates(subset=["brand", "name"], keep="first")
    return df

async def _harvest_from_dom(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    rank = 1
    # 카드 탐색
    cards = []
    for sel in PRODUCT_CARD_SELECTORS:
        cards = soup.select(sel)
        if cards:
            break

    for card in cards:
        name = _extract_text(card, NAME_SELECTORS)
        brand = _extract_text(card, BRAND_SELECTORS)
        link = _extract_link(card, LINK_SELECTORS)

        price_wrap = None
        for psel in PRICE_WRAP_SELECTORS:
            price_wrap = card.select_one(psel)
            if price_wrap:
                break
        sale, original = _extract_prices(price_wrap or card)
        discount = _calc_discount(sale, original)

        rows.append({
            "rank": rank,
            "brand": brand or "",
            "name": name or "",
            "price": sale if sale is not None else original,
            "price_str": f"${sale:.2f}" if sale is not None else (f"${original:.2f}" if original else ""),
            "original_price": original,
            "discount_pct": discount,
            "url": link,
        })
        rank += 1

    df = pd.DataFrame(rows)
    # 결측 정리
    df["name"] = df["name"].fillna("").astype(str)
    df["brand"] = df["brand"].fillna("").astype(str)
    # 100개까지만 슬라이스
    if not df.empty:
        df = df.iloc[:100].copy()
    df = _dedupe(df)
    return df

async def _scrape_impl(debug=False) -> pd.DataFrame:
    _ensure_debug_dirs()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/Los_Angeles",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            viewport={"width": 1380, "height": 900}
        )
        page = await context.new_page()

        # JSON 응답 수집 (best/best-seller 관련)
        json_payloads: List[Dict[str, Any]] = []
        def is_best_url(u: str) -> bool:
            u = u.lower()
            return ("best" in u and "seller" in u) or ("best" in u and "list" in u)

        page.on("response", lambda resp: asyncio.create_task(_collect_json(resp, json_payloads, is_best_url)))

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60000)
        await _wait_network_settle(page, 12000)

        # 지역(미국) 강제
        await _force_region_us(page)
        await _wait_network_settle(page, 12000)

        # 스크롤/더보기
        await _scroll_to_load(page, target_count=100)

        # DOM 기준 파싱
        html = await page.content()
        df_dom = await _harvest_from_dom(html)

        # JSON이 더 신뢰할 수 있으면 JSON 우선 (키 추론)
        df_json = _harvest_from_json(json_payloads)

        await context.close()
        await browser.close()

        # 선택 로직: JSON → DOM → 병합
        if df_json is not None and len(df_json) >= 50:
            df = df_json
        elif not df_dom.empty:
            df = df_dom
        else:
            df = pd.DataFrame([])

        if debug:
            stamp = int(time.time())
            with open(f"{DEBUG_DIR}/page_{stamp}.html", "w", encoding="utf-8") as f:
                f.write(html)
            df.to_csv(f"{DEBUG_DIR}/parsed_{stamp}.csv", index=False, encoding="utf-8-sig")

        # 최종 컬럼 정리
        if not df.empty:
            # 가격 정규화/문자열
            df["price_str"] = df.apply(
                lambda r: f"${float(r['price']):.2f}" if pd.notnull(r.get("price")) else (r.get("price_str") or ""),
                axis=1
            )
            # rank 보정
            df = df.head(100).copy()
            df["rank"] = range(1, len(df) + 1)
            # NaN 안전 처리
            for c in ["brand", "name", "url"]:
                if c in df.columns:
                    df[c] = df[c].fillna("").astype(str)

        return df

async def _collect_json(resp, acc: List[Dict[str, Any]], pred) -> None:
    try:
        if "application/json" in (resp.headers.get("content-type") or ""):
            url = resp.url
            if pred(url):
                data = await resp.json()
                acc.append({"url": url, "data": data})
    except Exception:
        pass

def _harvest_from_json(payloads: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """
    가능한 JSON 구조를 추론해 일반화 파싱.
    """
    for item in payloads:
        data = item.get("data")
        if not isinstance(data, (dict, list)):
            continue

        # 흔한 구조들 탐색: data -> list/products/items
        candidates = []
        if isinstance(data, dict):
            # 중첩 dict에서 상품 리스트로 보이는 부분 찾기
            for k, v in data.items():
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    candidates.append(v)
                elif isinstance(v, dict):
                    for k2, v2 in v.items():
                        if isinstance(v2, list) and len(v2) > 0 and isinstance(v2[0], dict):
                            candidates.append(v2)
        elif isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict):
                candidates.append(data)

        for cand in candidates:
            rows = []
            for i, prod in enumerate(cand, start=1):
                # 필드 추론
                name = prod.get("name") or prod.get("productName") or prod.get("goodsNm") or ""
                brand = prod.get("brand") or prod.get("brandName") or prod.get("brandNm") or ""
                url = prod.get("url") or prod.get("linkUrl") or prod.get("detailUrl") or ""
                if url and url.startswith("/"):
                    url = "https://global.oliveyoung.com" + url

                sale = None
                original = None
                # 다양한 가격 필드 대응
                for key in ["salePrice", "price", "saleAmt", "finalPrice", "goodsPrice"]:
                    v = prod.get(key)
                    if isinstance(v, (int, float)):
                        sale = float(v)
                        break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p:
                            sale = p
                            break
                for key in ["originPrice", "listPrice", "originalPrice", "marketPrice"]:
                    v = prod.get(key)
                    if isinstance(v, (int, float)):
                        original = float(v)
                        break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p:
                            original = p
                            break

                discount = None
                if sale and original and original > 0 and sale <= original:
                    discount = int(round((original - sale) / original * 100))

                rows.append({
                    "rank": i,
                    "brand": brand,
                    "name": name,
                    "price": sale if sale is not None else original,
                    "price_str": f"${sale:.2f}" if sale is not None else (f"${original:.2f}" if original else ""),
                    "original_price": original,
                    "discount_pct": discount,
                    "url": url,
                })
            if rows:
                df = pd.DataFrame(rows)
                return df
    return None

def scrape_oy_global_us(debug=False) -> pd.DataFrame:
    return asyncio.run(_scrape_impl(debug=debug))
