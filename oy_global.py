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

# ====== 제품 카드/필드 후보 ======
PRODUCT_CARD_SELECTORS = [
    'ul[class*="prd"] li', 'ul[class*="product"] li', 'li[data-product-id]',
    'li[class*="prd"]', 'li[class*="item"]', 'div[class*="prd"] li'
]
NAME_SELECTORS = [
    '.prod-name', '.name', '.tit', '.title', '.goods-name',
    'a[title]', 'img[alt]', '[aria-label]'
]
BRAND_SELECTORS = ['.brand', '.prod-brand', '.brand-name', '[data-brand-name]']
PRICE_WRAP_SELECTORS = ['.price', '.prod-price', '.price-area', '.cost', '.amount']
LINK_SELECTORS = ['a[href]']

DEBUG_DIR = "data/debug"

# ====== URL 필터 (상품 상세만 허용) ======
ALLOW_RE = re.compile(r'/(product|goods)[/].*detail|goods(No|no)=|prd(No|no)=', re.I)
DENY_RE = re.compile(
    r'/member/|/myaccount/|/account|/brand($|/)|/display/page/|/event/|/flash-deal|/new-arrivals|/category/|/search',
    re.I
)
ONCLICK_DETAIL_RE = re.compile(r"""['"](?P<url>/(?:product|goods)[^'"]+)['"]""")

def _ensure_debug_dirs():
    os.makedirs(DEBUG_DIR, exist_ok=True)

def _normalize_href(h: str) -> str:
    if not h: return ""
    if h.startswith("//"): h = "https:" + h
    if h.startswith("/"):  h = "https://global.oliveyoung.com" + h
    return h

def _is_product_link(href: str) -> bool:
    if not href: return False
    if DENY_RE.search(href): return False
    return bool(ALLOW_RE.search(href))

def _text_from_attrs(tag) -> str:
    for attr in ["aria-label", "title", "data-name", "data-goods-nm", "data-product-name"]:
        if tag and tag.has_attr(attr) and tag[attr].strip():
            return tag[attr].strip()
    return ""

def _extract_text(el, selectors: List[str]) -> str:
    for sel in selectors:
        t = el.select_one(sel)
        if t:
            txt = (t.get_text(strip=True) or _text_from_attrs(t) or t.get("alt", "").strip())
            if txt: return txt
    raw = el.get_text("\n", strip=True)
    if raw:
        parts = [p for p in (s.strip() for s in raw.split("\n")) if p]
        parts.sort(key=len, reverse=True)
        return parts[0] if parts else ""
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

def _recover_href_from_onclick(tag) -> str:
    oc = tag.get("onclick", "")
    m = ONCLICK_DETAIL_RE.search(oc)
    return _normalize_href(m.group("url")) if m else ""

def _recover_href_from_data(el) -> str:
    for a in ["data-url", "data-href", "data-link", "data-detail-url", "data-product-url"]:
        if el.has_attr(a) and el[a]:
            return _normalize_href(el[a])
    for a in ["data-product-id", "data-goods-no", "data-ref-goodsno", "data-prd-no", "data-prdno", "data-goodsno"]:
        if el.has_attr(a) and el[a]:
            return _normalize_href(f"/product/detail?prdNo={el[a]}")
    return ""

def _extract_link(card) -> str:
    # 1) card 내부 모든 a 검사
    for a in card.select('a[href], a[onclick]'):
        href = a.get("href", "").strip()
        if href and href != "javascript:;" and href != "javascript:void(0)":
            href_n = _normalize_href(href)
            if _is_product_link(href_n):
                return href_n
        # onclick에서 복원
        oc_href = _recover_href_from_onclick(a)
        if _is_product_link(oc_href):
            return oc_href
        # data-*에서 복원
        for k in ["data-url","data-href","data-link","data-detail-url","data-product-url"]:
            if a.has_attr(k) and a[k]:
                h = _normalize_href(a[k])
                if _is_product_link(h): return h
    # 2) 카드 루트 data-*에서 복원
    data_href = _recover_href_from_data(card)
    if _is_product_link(data_href): return data_href
    return ""

async def _harvest_from_dom(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows, rank = [], 1

    # 후보 카드 집합
    cards = []
    for sel in PRODUCT_CARD_SELECTORS:
        found = soup.select(sel)
        if found and len(found) >= 4:
            cards = found
            break
    # 앵커 기반 보정: 전체 a에서 제품 상세만 골라 상위 120개 근처의 a들을 카드로 취급
    if not cards:
        anchors = soup.select('a[href], a[onclick]')
        prod_anchors = []
        for a in anchors:
            href = a.get("href", "").strip()
            href_n = _normalize_href(href) if href else ""
            ok = _is_product_link(href_n)
            if not ok:
                oc_href = _recover_href_from_onclick(a)
                ok = _is_product_link(oc_href)
            if ok:
                prod_anchors.append(a)
        # 부모 li/div를 카드로
        parents = []
        for a in prod_anchors[:150]:
            p = a
            for _ in range(4):
                if p and p.name not in ("li", "div"):
                    p = p.parent
                else:
                    break
            if p and p.name in ("li", "div"):
                parents.append(p)
        cards = parents or prod_anchors

    for card in cards:
        name = _extract_text(card, NAME_SELECTORS)
        brand = _extract_text(card, BRAND_SELECTORS)
        link = _extract_link(card)

        # URL 필터 최종 체크
        if not _is_product_link(link):
            continue

        price_wrap = None
        for psel in PRICE_WRAP_SELECTORS:
            price_wrap = card.select_one(psel)
            if price_wrap: break
        sale, original = _extract_prices(price_wrap or card)
        discount = _calc_discount(sale, original)

        rows.append({
            "rank": rank, "brand": brand or "", "name": name or "",
            "price": sale if sale is not None else original,
            "price_str": f"${sale:.2f}" if sale is not None else (f"${original:.2f}" if original else ""),
            "original_price": original, "discount_pct": discount, "url": link,
        })
        rank += 1
        if rank > 120: break

    df = pd.DataFrame(rows)
    if not df.empty:
        # 100개까지만 + 정제
        df = df[df["name"].str.len() > 0].copy()
        df = _dedupe(df).head(100).reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)
        for c in ["brand","name","url"]:
            df[c] = df[c].fillna("").astype(str)
    return df

# ====== XHR 수집 및 파싱 ======
def _flatten_lists_from_json(obj) -> List[List[Dict[str, Any]]]:
    """임의의 JSON에서 '상품스멜'이 나는 dict 리스트들을 재귀적으로 수집."""
    hits = []
    keys_smell = {"goodsNo","goodsno","prdNo","productNo","goodsNm","productName","brandName","name"}
    def walk(x):
        if isinstance(x, list) and x and isinstance(x[0], dict):
            # 상품스멜
            sample = x[0]
            if any(k in sample for k in keys_smell):
                hits.append(x)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)
    walk(obj)
    return hits

def _build_url_from_item(item: Dict[str, Any]) -> str:
    for k in ["url","linkUrl","detailUrl"]:
        if item.get(k):
            return _normalize_href(item[k])
    for k in ["prdNo","productNo","goodsNo","goodsno","prdnm"]:
        v = item.get(k)
        if v: return _normalize_href(f"/product/detail?prdNo={v}")
    return ""

def _num_from_any(x):
    try:
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _harvest_from_json_payloads(payloads: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    rows = []
    for it in payloads:
        data = it.get("data")
        if data is None: continue
        for lst in _flatten_lists_from_json(data):
            for i, prod in enumerate(lst, start=1):
                name = prod.get("name") or prod.get("productName") or prod.get("goodsNm") or ""
                brand = prod.get("brand") or prod.get("brandName") or ""
                url = _build_url_from_item(prod)

                sale = None
                for k in ["salePrice","price","saleAmt","finalPrice","goodsPrice","sale_price"]:
                    v = prod.get(k)
                    n = _num_from_any(v)
                    if n is not None: sale = n; break
                original = None
                for k in ["originPrice","listPrice","originalPrice","marketPrice","ori_price"]:
                    v = prod.get(k)
                    n = _num_from_any(v)
                    if n is not None: original = n; break

                disc = None
                if sale and original and original>0 and sale<=original:
                    disc = int(round((original-sale)/original*100))

                rows.append({
                    "rank": i, "brand": brand, "name": name,
                    "price": sale if sale is not None else original,
                    "price_str": f"${sale:.2f}" if sale is not None else (f"${original:.2f}" if original else ""),
                    "original_price": original, "discount_pct": disc, "url": url
                })
    if not rows: return None
    df = pd.DataFrame(rows)
    df = df[df["url"].map(_is_product_link)].copy()
    if df.empty: return None
    df = _dedupe(df).head(100).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    for c in ["brand","name","url"]:
        df[c] = df[c].fillna("").astype(str)
    return df

@retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
async def _wait_dom_ready(page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(0.5)
    try:
        await page.wait_for_selector("body", timeout=3000)
    except PWTimeout:
        pass

async def _route_block(route):
    try:
        url = route.request.url
        if any(url.endswith(ext) for ext in (".png",".jpg",".jpeg",".gif",".webp",".svg",".woff",".woff2",".ttf",".mp4",".webm")):
            return await route.abort()
        if route.request.resource_type in ("image","font","media"):
            return await route.abort()
        return await route.continue_()
    except Exception:
        try: await route.continue_()
        except Exception: pass

async def _scrape_impl(debug=False) -> pd.DataFrame:
    os.makedirs(DEBUG_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage",
        ])
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            viewport={"width": 1280, "height": 900},
            timezone_id="Asia/Seoul",
        )
        await context.route("**/*", lambda r: asyncio.create_task(_route_block(r)))
        page = await context.new_page()

        # 모든 JSON 수집
        json_payloads: List[Dict[str, Any]] = []
        page.on("response", lambda resp: asyncio.create_task(_collect_any_json(resp, json_payloads)))

        await _wait_dom_ready(page, BEST_URL)

        # 가벼운 스크롤 (최대 10라운드, 100개 도달 시 중단)
        for _ in range(10):
            await page.evaluate("window.scrollBy(0, 1600)")
            await asyncio.sleep(0.35)

        html = await page.content()
        df_json = _harvest_from_json_payloads(json_payloads)
        df_dom = await _harvest_from_dom(html)

        if debug:
            stamp = int(time.time())
            with open(f"{DEBUG_DIR}/page_{stamp}.html","w",encoding="utf-8") as f: f.write(html)
            if df_dom is not None and not df_dom.empty:
                df_dom.to_csv(f"{DEBUG_DIR}/parsed_dom_{stamp}.csv", index=False, encoding="utf-8-sig")
            if df_json is not None and not df_json.empty:
                df_json.to_csv(f"{DEBUG_DIR}/parsed_json_{stamp}.csv", index=False, encoding="utf-8-sig")

        await context.close(); await browser.close()

        # JSON 우선, 없으면 DOM
        if df_json is not None and not df_json.empty:
            df = df_json
        else:
            df = df_dom

        if df is None:
            df = pd.DataFrame([])
        # 최종 보호
        if not df.empty:
            df = df.head(100).copy()
            df["rank"] = range(1, len(df)+1)
            df["price_str"] = df.apply(
                lambda r: f"${float(r['price']):.2f}" if pd.notnull(r.get("price")) else (r.get("price_str") or ""),
                axis=1
            )
            for c in ["brand","name","url"]:
                df[c] = df[c].fillna("").astype(str)
        return df

async def _collect_any_json(resp, acc: List[Dict[str, Any]]):
    try:
        ctype = (resp.headers.get("content-type") or "")
        if "application/json" in ctype:
            data = await resp.json()
            acc.append({"url": resp.url, "data": data})
    except Exception:
        pass

def scrape_oy_global_us(debug=False) -> pd.DataFrame:
    return asyncio.run(_scrape_impl(debug=debug))
