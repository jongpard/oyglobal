import asyncio
import json
import os
import re
import time
from typing import List, Dict, Any, Optional

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from tenacity import retry, stop_after_attempt, wait_fixed

from price_parser import parse_price

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# 다중 셀렉터 후보 (구조 변경 대응)
PRODUCT_CARD_SELECTORS = [
    'li[data-product-id]',
    'ul[class*="prd"] li',
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
    미국 기준 노출을 위해 가능한 모든 방법을 시도.
    기본값: 비활성. OY_FORCE_US=1 일 때만 실행.
    """
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
    await _click_if_exists(page, [
        'li:has-text("United States")',
        'button:has-text("United States")',
        'text=United States'
    ])
    await _click_if_exists(page, ['button:has-text("Save")', 'button:has-text("Apply")', 'button:has-text("OK")'])

async def _soft_wait_networkidle(page, timeout_ms=8000):
    """
    networkidle 은 SPA 에서 영원히 오지 않는 경우가 많음 → 소프트 대기.
    시간 초과시 예외를 올리지 않고 그냥 반환.
    """
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PWTimeout:
        return

async def _wait_for_any_selector(page, selectors: List[str], total_timeout_ms: int = 30000) -> None:
    """
    여러 후보 셀렉터 중 하나라도 나타날 때까지 대기.
    """
    deadline = time.time() + (total_timeout_ms / 1000.0)
    last_errs = []
    while time.time() < deadline:
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=1000, state="attached")
                return
            except PWTimeout as e:
                last_errs.append((sel, str(e)))
        await asyncio.sleep(0.2)
    # 전부 실패 시 마지막 에러 일부만 보여줌
    sample = "; ".join([f"{s}" for s, _ in last_errs[-5:]])
    raise PWTimeout(f"Timed out waiting for any product selector. Tried: {sample}")

async def _scroll_to_load(page, target_count=100, step_px=2000, max_rounds=30):
    """
    무한 스크롤/지연 로딩 대응: 고정 sleep + DOM 변화 체크.
    """
    last_height = 0
    for i in range(max_rounds):
        await page.evaluate(f"window.scrollBy(0, {step_px});")
        await asyncio.sleep(0.8)
        await _soft_wait_networkidle(page, 4000)
        try:
            height = await page.evaluate("document.body.scrollHeight")
        except Exception:
            height = last_height
        if height == last_height:
            # 더보기 버튼이 있을 수 있으니 한 번 눌러본다
            clicked = await _click_if_exists(page, ['button:has-text("More")', 'button:has-text("더보기")'])
            if not clicked:
                break
        last_height = height
        # 카드 개수 확인
        cards_count = 0
        for sel in PRODUCT_CARD_SELECTORS:
            try:
                cards_count = max(cards_count, await page.locator(sel).count())
            except Exception:
                pass
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
    sale = parse_price(text)
    strike = el.select_one('del, .origin, .original, .strike, .price-origin')
    original = parse_price(strike.get_text(" ", strip=True)) if strike else None
    if original and sale and original < sale:
        sale, original = original, sale
    return sale, original

def _calc_discount(sale: Optional[float], original: Optional[float]) -> Optional[int]:
    if sale and original and original > 0 and sale <= original:
        pct = round((original - sale) / original * 100)
        return int(pct)
    return None

def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")
    df = df.drop_duplicates(subset=["brand", "name"], keep="first")
    return df

async def _harvest_from_dom(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    rank = 1
    cards: List = []
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
    df["name"] = df["name"].fillna("").astype(str)
    df["brand"] = df["brand"].fillna("").astype(str)
    if not df.empty:
        df = df.iloc[:100].copy()
    df = _dedupe(df)
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
    for item in payloads:
        data = item.get("data")
        if not isinstance(data, (dict, list)):
            continue

        candidates = []
        if isinstance(data, dict):
            for _, v in data.items():
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    candidates.append(v)
                elif isinstance(v, dict):
                    for __, v2 in v.items():
                        if isinstance(v2, list) and len(v2) > 0 and isinstance(v2[0], dict):
                            candidates.append(v2)
        elif isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict):
                candidates.append(data)

        for cand in candidates:
            rows = []
            for i, prod in enumerate(cand, start=1):
                name = prod.get("name") or prod.get("productName") or prod.get("goodsNm") or ""
                brand = prod.get("brand") or prod.get("brandName") or prod.get("brandNm") or ""
                url = prod.get("url") or prod.get("linkUrl") or prod.get("detailUrl") or ""
                if url and url.startswith("/"):
                    url = "https://global.oliveyoung.com" + url

                sale = None
                original = None
                for key in ["salePrice", "price", "saleAmt", "finalPrice", "goodsPrice"]:
                    v = prod.get(key)
                    if isinstance(v, (int, float)):
                        sale = float(v); break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p: sale = p; break
                for key in ["originPrice", "listPrice", "originalPrice", "marketPrice"]:
                    v = prod.get(key)
                    if isinstance(v, (int, float)):
                        original = float(v); break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p: original = p; break

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
                return pd.DataFrame(rows)
    return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def _wait_dom_ready(page, url: str):
    """
    초기 진입 안정화: domcontentloaded → 제품 카드 후보 셀렉터 등장까지.
    """
    # domcontentloaded 까지만 강제. networkidle 은 소프트로만.
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await _soft_wait_networkidle(page, 6000)
    await _wait_for_any_selector(page, PRODUCT_CARD_SELECTORS, total_timeout_ms=35000)

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
            # locale/timezone 은 기본값으로 두되, 필요시 헤더만 유지
            timezone_id="Asia/Seoul",
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

        # 진입 및 초기 DOM 대기
        await _wait_dom_ready(page, BEST_URL)

        # 요청 시에만 미국 강제 (기본 비활성)
        if os.getenv("OY_FORCE_US", "0") == "1":
            await _force_region_us(page)
            await _soft_wait_networkidle(page, 6000)
            # 지역 변경 후에도 다시 카드 등장 확인
            await _wait_for_any_selector(page, PRODUCT_CARD_SELECTORS, total_timeout_ms=20000)

        # 스크롤로 충분히 로드
        await _scroll_to_load(page, target_count=100)

        # DOM 기준 파싱
        html = await page.content()
        df_dom = await _harvest_from_dom(html)

        # JSON이 더 신뢰되면 JSON 우선
        df_json = _harvest_from_json(json_payloads)

        await context.close()
        await browser.close()

        # 선택 로직
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

        # 최종 정리
        if not df.empty:
            df["price_str"] = df.apply(
                lambda r: f"${float(r['price']):.2f}" if pd.notnull(r.get("price")) else (r.get("price_str") or ""),
                axis=1
            )
            df = df.head(100).copy()
            df["rank"] = range(1, len(df) + 1)
            for c in ["brand", "name", "url"]:
                if c in df.columns:
                    df[c] = df[c].fillna("").astype(str)

        return df

def scrape_oy_global_us(debug=False) -> pd.DataFrame:
    return asyncio.run(_scrape_impl(debug=debug))
