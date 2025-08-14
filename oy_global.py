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

PRODUCT_CARD_SELECTORS = [
    'li[data-product-id]',
    'ul[class*="prd"] li',
    'ul[class*="list"] li',
    'div[class*="product"] li',
    'li[class*="item"]',
]
NAME_SELECTORS = [
    '.prod-name', '.name', '.tit', '.title', '.goods-name',
    'a[title]', 'img[alt]', '[aria-label]'
]
BRAND_SELECTORS = [
    '.brand', '.prod-brand', '.brand-name', '[data-brand-name]'
]
PRICE_WRAP_SELECTORS = [
    '.price', '.prod-price', '.price-area', '.cost', '.amount'
]
LINK_SELECTORS = [
    'a[href*="/product/"]', 'a[href*="/goods/"]', 'a[href]'
]

DEBUG_DIR = "data/debug"

def _ensure_debug_dirs():
    os.makedirs(DEBUG_DIR, exist_ok=True)

async def _click_if_exists(page, selectors_or_text: List[str]) -> bool:
    for sel in selectors_or_text:
        try:
            if sel.startswith("text="):
                locator = page.get_by_text(sel.replace("text=", ""), exact=False)
                if await locator.count() > 0:
                    await locator.first.click(); return True
            else:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click(); return True
        except Exception:
            pass
    return False

async def _soft_wait_networkidle(page, timeout_ms=6000):
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PWTimeout:
        return

async def _wait_for_any_selector(page, selectors: List[str], total_timeout_ms: int = 20000) -> None:
    deadline = time.time() + (total_timeout_ms / 1000.0)
    while time.time() < deadline:
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=800, state="attached")
                return
            except PWTimeout:
                pass
        await asyncio.sleep(0.15)
    raise PWTimeout("Timed out waiting product cards.")

async def _scroll_to_load(page, target_count=100, step_px=1600, max_rounds=18):
    last_count = 0
    for _ in range(max_rounds):
        await page.evaluate(f"window.scrollBy(0, {step_px});")
        await asyncio.sleep(0.4)
        await _soft_wait_networkidle(page, 2500)
        count = 0
        for sel in PRODUCT_CARD_SELECTORS:
            try:
                count = max(count, await page.locator(sel).count())
            except Exception:
                pass
        if count >= target_count: break
        if count == last_count:
            break
        last_count = count

def _text_from_attrs(tag) -> str:
    for attr in ["aria-label", "title", "data-name", "data-goods-nm", "data-product-name"]:
        if tag and tag.has_attr(attr) and tag[attr].strip():
            return tag[attr].strip()
    return ""

def _extract_text(el, selectors: List[str]) -> str:
    for sel in selectors:
        target = el.select_one(sel)
        if target:
            txt = target.get_text(strip=True)
            if not txt:
                txt = _text_from_attrs(target)
            if not txt and target.has_attr("alt"):
                txt = target["alt"].strip()
            if txt:
                return txt
    raw = el.get_text("\n", strip=True)
    if raw:
        parts = [p.strip() for p in raw.split("\n") if p.strip()]
        parts.sort(key=len, reverse=True)
        return parts[0] if parts else ""
    return ""

_DETAIL_RE = re.compile(r"""['"](?P<url>/(?:product|goods)[^'"]+)['"]""")

def _normalize_href(href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        href = "https://global.oliveyoung.com" + href
    return href

def _extract_link(el, selectors: List[str]) -> str:
    for sel in selectors:
        tag = el.select_one(sel)
        if tag and tag.has_attr("href"):
            href = tag["href"].strip()
            if "javascript:void" not in href:
                return _normalize_href(href)
            for a in ["data-url", "data-href", "data-link", "data-detail-url"]:
                if tag.has_attr(a) and tag[a]:
                    return _normalize_href(tag[a])
            if tag.has_attr("onclick"):
                m = _DETAIL_RE.search(tag["onclick"])
                if m:
                    return _normalize_href(m.group("url"))
    for a in ["data-url", "data-href", "data-link", "data-detail-url", "data-product-url"]:
        if el.has_attr(a) and el[a]:
            return _normalize_href(el[a])
    for a in ["data-product-id", "data-goods-no", "data-ref-goodsno", "data-prd-no"]:
        if el.has_attr(a) and el[a]:
            return _normalize_href(f"/product/detail?prdNo={el[a]}")
    return ""

def _extract_prices(el) -> (Optional[float], Optional[float]):
    text = el.get_text(" ", strip=True)
    sale = parse_price(text)
    strike = el.select_one('del, .origin, .original, .strike, .price-origin')
    original = parse_price(strike.get_text(" ", strip=True)) if strike else None
    if original and sale and original < sale:
        sale, original = original, sale
    return sale, original

def _calc_discount(sale: Optional[float], original: Optional[float]) -> Optional[int]:
    if sale and original and original > 0 and sale <= original:
        return int(round((original - sale) / original * 100))
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
        if cards: break

    for card in cards:
        name = _extract_text(card, NAME_SELECTORS)
        brand = _extract_text(card, BRAND_SELECTORS)
        link = _extract_link(card, LINK_SELECTORS)

        price_wrap = None
        for psel in PRICE_WRAP_SELECTORS:
            price_wrap = card.select_one(psel)
            if price_wrap: break
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
    if not df.empty:
        df["name"] = df["name"].fillna("").astype(str)
        df["brand"] = df["brand"].fillna("").astype(str)
        df = df.iloc[:100].copy()
    return _dedupe(df)

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
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    candidates.append(v)
                elif isinstance(v, dict):
                    for __, v2 in v.items():
                        if isinstance(v2, list) and v2 and isinstance(v2[0], dict):
                            candidates.append(v2)
        elif isinstance(data, list) and data and isinstance(data[0], dict):
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
                    if isinstance(v, (int, float)): sale = float(v); break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p: sale = p; break
                for key in ["originPrice", "listPrice", "originalPrice", "marketPrice"]:
                    v = prod.get(key)
                    if isinstance(v, (int, float)): original = float(v); break
                    if isinstance(v, str):
                        p = parse_price(v)
                        if p: original = p; break
                discount = _calc_discount(sale, original)
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

@retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
async def _wait_dom_ready(page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await _soft_wait_networkidle(page, 3000)
    await _wait_for_any_selector(page, PRODUCT_CARD_SELECTORS, total_timeout_ms=15000)

async def _route_block(route):
    try:
        url = route.request.url
        if any(url.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf")):
            return await route.abort()
        res_type = route.request.resource_type
        if res_type in ("image", "font", "media"):
            return await route.abort()
        return await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass

async def _scrape_impl(debug=False) -> pd.DataFrame:
    _ensure_debug_dirs()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])

        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            viewport={"width": 1280, "height": 900},
            timezone_id="Asia/Seoul",
        )

        await context.route("**/*", lambda route: asyncio.create_task(_route_block(route)))
        page = await context.new_page()

        # XHR 수집기
        json_payloads: List[Dict[str, Any]] = []
        def is_best_url(u: str) -> bool:
            u = u.lower()
            return ("best" in u and ("seller" in u or "list" in u))
        page.on("response", lambda resp: asyncio.create_task(_collect_json(resp, json_payloads, is_best_url)))

        # 진입 & 카드 대기
        await _wait_dom_ready(page, BEST_URL)

        # 👉 문제 지점 수정: wait_for_response 완전 제거
        #    이벤트 수집으로 충분 + 짧게 숨 고르기
        await asyncio.sleep(2.0)

        # 스크롤 최소화
        await _scroll_to_load(page, target_count=100, max_rounds=12)

        # DOM / XHR 파싱
        html = await page.content()
        df_dom = await _harvest_from_dom(html)
        df_json = _harvest_from_json(json_payloads)

        # 안전 종료
        await context.close()
        await browser.close()

        # 선택: JSON 우선
        if df_json is not None and len(df_json) >= 30:
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
