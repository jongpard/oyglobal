import asyncio, json, os, re, time
from typing import List, Dict, Any, Optional
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from tenacity import retry, stop_after_attempt, wait_fixed
from price_parser import parse_price

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# ----- 후보 셀렉터 -----
PRODUCT_CARD_SELECTORS = [
    'ul[class*="prd"] li', 'ul[class*="product"] li',
    'li[data-product-id]', 'li[class*="prd"]', 'li[class*="item"]',
    'div[class*="prd"] li'
]
NAME_SELECTORS  = [
    '.prod-name','.name','.tit','.title','.goods-name',
    'a[title]','img[alt]','[aria-label]'
]
# 브랜드 배지/라벨 후보(상단 작은 칩 포함)
BRAND_SELECTORS = [
    '.brand','.prod-brand','.brand-name','[data-brand-name]',
    '.badge.brand','.flag-brand','em.brand','strong.brand',
    'span[class*="brand"]','div[class*="brand"]','p[class*="brand"]',
    'a[class*="brand"]','a[href*="/brand/"]'
]
PRICE_WRAP_SELECTORS = ['.price','.prod-price','.price-area','.cost','.amount']

DEBUG_DIR = "data/debug"

# ----- URL 필터: 상품 상세만 허용 -----
ALLOW_RE = re.compile(r'/(product|goods)[/].*detail|goods(No|no)=|prd(No|no)=', re.I)
DENY_RE  = re.compile(
    r'/member/|/myaccount/|/account|/brand($|/)|/display/page/|/event/|/flash-deal|/new-arrivals|/category/|/search',
    re.I
)
ONCLICK_DETAIL_RE = re.compile(r"""['"](?P<url>/(?:product|goods)[^'"]+)['"]""")

# 브랜드로 오인되기 쉬운 키워드(대괄호/괄호 안 텍스트 걸러내기)
NON_BRAND_WORD_RE = re.compile(
    r'(set|twin\s*pack|pack|types?|refill|supply|gift|edition|exclusive|collab|special|cream|ml|ea|pcs?)',
    re.I
)

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
    for a in ["aria-label","title","data-name","data-goods-nm","data-product-name"]:
        if tag and tag.has_attr(a) and tag[a].strip():
            return tag[a].strip()
    return ""

def _clean_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()

def _extract_name(card) -> str:
    for sel in NAME_SELECTORS:
        t = card.select_one(sel)
        if t:
            txt = t.get_text(" ", strip=True) or _text_from_attrs(t) or t.get("alt","")
            txt = _clean_spaces(txt)
            if txt: return txt
    raw = _clean_spaces(card.get_text(" ", strip=True))
    return raw

def _brand_from_badge(card) -> str:
    # 클래스명에 brand 포함된 모든 엘리먼트 탐색
    for el in card.find_all(True, recursive=True):
        classes = " ".join(el.get("class", [])).lower()
        if "brand" in classes:
            txt = _clean_spaces(el.get_text(" ", strip=True))
            if txt and not NON_BRAND_WORD_RE.search(txt):
                return txt
    # 셀렉터 직접 조회 (보수)
    for sel in BRAND_SELECTORS:
        el = card.select_one(sel)
        if el:
            txt = _clean_spaces(el.get_text(" ", strip=True) or _text_from_attrs(el))
            if txt and not NON_BRAND_WORD_RE.search(txt):
                return txt
    return ""

def _brand_from_name_fallback(name_after: str) -> str:
    # [브랜드] ... / (...) 꼬리에서 브랜드 후보 뽑기 (금지어 포함시 버림)
    in_brackets = re.findall(r'\[([^\]]+)\]', name_after) + re.findall(r'\(([^\)]+)\)', name_after)
    for cand in reversed(in_brackets):  # 뒤에서부터(브랜드가 뒤에 오는 케이스 우선)
        cand = _clean_spaces(cand)
        if cand and not NON_BRAND_WORD_RE.search(cand) and len(cand) <= 30:
            return cand
    # 첫 토큰이 브랜드일 가능성 (예: VT, AHC, numbuzin)
    first = (name_after.split() or [""])[0]
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9\.\-&']{1,20}", first):
        return first
    return ""

def _extract_brand(card, name_after: str) -> str:
    b = _brand_from_badge(card)
    if b: return b
    b = _brand_from_name_fallback(name_after)
    return b or ""

def _recover_href_from_onclick(tag) -> str:
    oc = tag.get("onclick","")
    m = ONCLICK_DETAIL_RE.search(oc)
    return m.group("url") if m else ""

def _recover_href_from_data(el) -> str:
    for a in ["data-url","data-href","data-link","data-detail-url","data-product-url"]:
        if el.has_attr(a) and el[a]: return el[a]
    for a in ["data-product-id","data-goods-no","data-ref-goodsno","data-prd-no","data-prdno","data-goodsno"]:
        if el.has_attr(a) and el[a]: return f"/product/detail?prdNo={el[a]}"
    return ""

def _extract_link(card) -> str:
    for a in card.select('a[href], a[onclick]'):
        href = a.get("href","").strip()
        if href and "javascript" not in href:
            h = _normalize_href(href)
            if _is_product_link(h): return h
        oc = _normalize_href(_recover_href_from_onclick(a))
        if _is_product_link(oc): return oc
        for k in ["data-url","data-href","data-link","data-detail-url","data-product-url"]:
            if a.has_attr(k) and a[k]:
                h = _normalize_href(a[k])
                if _is_product_link(h): return h
    h2 = _normalize_href(_recover_href_from_data(card))
    if _is_product_link(h2): return h2
    return ""

# ---- 가격 추출 (정가/할인가 분리) ----
_PRICE_CLASS_HINT = re.compile(r"(sale|final|now|current|sell|price|pay|cost)", re.I)

def _extract_prices(card) -> (Optional[float], Optional[float]):
    # 가격 블록
    wrap = None
    for s in PRICE_WRAP_SELECTORS:
        wrap = card.select_one(s)
        if wrap: break
    target = wrap or card

    # 1) 명시적 정가(del 등)
    original = None
    strike = target.select_one('del, .origin, .original, .strike, .price-origin, .consumer, .normal-price')
    if strike:
        original = parse_price(strike.get_text(" ", strip=True))

    # 2) 할인가: class 힌트를 가진 노드 중 del 아닌 곳
    sale = None
    sale_cands = []
    for t in target.find_all(True):
        if t.name in ("script","style"): continue
        classes = " ".join(t.get("class", [])).lower()
        if _PRICE_CLASS_HINT.search(classes) and t.name != "del":
            val = parse_price(t.get_text(" ", strip=True))
            if val is not None:
                sale_cands.append(val)
    if sale_cands:
        sale = min(sale_cands)

    # 3) 후보 보정: 텍스트 전체에서 수치 모으기
    nums = []
    for t in target.find_all(True):
        if t.name in ("script","style"): continue
        txt = t.get_text(" ", strip=True)
        v = parse_price(txt)
        if v is not None:
            nums.append(v)
    nums = [v for v in nums if v > 0]
    if (sale is None or original is None) and nums:
        if len(nums) >= 2:
            lo, hi = min(nums), max(nums)
            sale = lo if sale is None else sale
            original = hi if original is None else original
        elif len(nums) == 1:
            sale = nums[0] if sale is None else sale

    # 순서 보정
    if sale and original and sale > original:
        sale, original = original, sale

    return sale, original

def _calc_discount(sale: Optional[float], original: Optional[float]) -> Optional[float]:
    if sale and original and original > 0 and sale <= original - 1e-6:
        return round((original - sale) / original * 100, 2)
    return None

def _dedupe(df: pd.DataFrame) -> pd.DataFrame:
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")
    df = df.drop_duplicates(subset=["brand","name"], keep="first")
    return df

async def _harvest_from_dom(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows, rank = [], 1

    # 카드 수집
    cards = []
    for sel in PRODUCT_CARD_SELECTORS:
        found = soup.select(sel)
        if found and len(found) >= 4:
            cards = found; break
    if not cards:
        anchors = soup.select('a[href], a[onclick]')
        prod_anchors = []
        for a in anchors:
            h = a.get("href","").strip()
            hn = _normalize_href(h) if h else ""
            ok = _is_product_link(hn) or _is_product_link(_normalize_href(_recover_href_from_onclick(a)))
            if ok: prod_anchors.append(a)
        parents = []
        for a in prod_anchors[:150]:
            p = a
            for _ in range(4):
                if p and p.name not in ("li","div"): p = p.parent
                else: break
            if p and p.name in ("li","div"): parents.append(p)
        cards = parents or prod_anchors

    for card in cards:
        raw_name = _extract_name(card)
        raw_name = _clean_spaces(raw_name)
        link = _extract_link(card)
        if not _is_product_link(link): continue
        brand = _extract_brand(card, raw_name)
        sale, original = _extract_prices(card)
        disc = _calc_discount(sale, original)
        rows.append({
            "rank": rank,
            "brand": brand or "",
            "name": raw_name or "",
            "original_price": original,
            "sale_price": sale,
            "discount_pct": disc,
            "url": link,
            "raw_name": raw_name or "",
        })
        rank += 1
        if rank > 120: break

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["name"].str.len() > 0].copy()
        df = _dedupe(df).head(100).reset_index(drop=True)
        df["rank"] = range(1, len(df)+1)
        for c in ["brand","name","url","raw_name"]:
            df[c] = df[c].fillna("").astype(str)
    return df

# -------- XHR 파싱 (전수 수집) --------
def _flatten_lists_from_json(obj) -> List[List[Dict[str, Any]]]:
    hits, keys = [], {"goodsNo","goodsno","prdNo","productNo","goodsNm","productName","brandName","name"}
    def walk(x):
        if isinstance(x, list) and x and isinstance(x[0], dict):
            if any(k in x[0] for k in keys): hits.append(x)
        elif isinstance(x, dict):
            for v in x.values(): walk(v)
    walk(obj); return hits

def _num(x):
    try: return float(str(x).replace(",","").strip())
    except Exception: return None

def _url_from_item(it: Dict[str,Any]) -> str:
    for k in ["url","linkUrl","detailUrl"]:
        if it.get(k): return _normalize_href(it[k])
    for k in ["prdNo","productNo","goodsNo","goodsno"]:
        v = it.get(k)
        if v: return _normalize_href(f"/product/detail?prdNo={v}")
    return ""

def _harvest_from_json_payloads(payloads: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    rows = []
    for it in payloads:
        data = it.get("data")
        if data is None: continue
        for lst in _flatten_lists_from_json(data):
            for i, prod in enumerate(lst, start=1):
                name  = prod.get("name") or prod.get("productName") or prod.get("goodsNm") or ""
                brand = prod.get("brand") or prod.get("brandName") or ""
                url   = _url_from_item(prod)

                sale = None
                for k in ["salePrice","price","saleAmt","finalPrice","goodsPrice","sale_price"]:
                    n = _num(prod.get(k))
                    if n is not None:
                        sale = n; break

                ori = None
                for k in ["originPrice","listPrice","originalPrice","marketPrice","ori_price"]:
                    n = _num(prod.get(k))
                    if n is not None:
                        ori = n; break

                disc = round((ori-sale)/ori*100, 2) if sale and ori and ori>0 and sale<=ori else None
                rows.append({
                    "rank": i, "brand": brand, "name": _clean_spaces(name),
                    "original_price": ori, "sale_price": sale, "discount_pct": disc,
                    "url": url, "raw_name": _clean_spaces(name)
                })
    if not rows: return None
    df = pd.DataFrame(rows)
    df = df[df["url"].map(_is_product_link)].copy()
    if df.empty: return None
    df = _dedupe(df).head(100).reset_index(drop=True)
    df["rank"] = range(1, len(df)+1)
    for c in ["brand","name","url","raw_name"]:
        df[c] = df[c].fillna("").astype(str)
    return df

# -------- Playwright --------
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

async def _click_if_exists(page, texts_or_sel: List[str]) -> bool:
    for t in texts_or_sel:
        try:
            if t.startswith("text="):
                loc = page.get_by_text(t.replace("text=",""), exact=False)
                if await loc.count() > 0: await loc.first.click(); return True
            else:
                loc = page.locator(t)
                if await loc.count() > 0: await loc.first.click(); return True
        except Exception: pass
    return False

async def _force_region_kr(page):
    await _click_if_exists(page, ['button:has-text("배송지")','a:has-text("배송지")','button:has-text("Ship to")','a:has-text("Ship to")','text=배송지','text=Ship to'])
    await _click_if_exists(page, ['li:has-text("대한민국")','button:has-text("대한민국")','text=대한민국','li:has-text("Korea")','button:has-text("Korea")','text=Korea'])
    await _click_if_exists(page, ['button:has-text("저장")','button:has-text("확인")','button:has-text("Save")','button:has-text("Apply")'])

@retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
async def _wait_dom_ready(page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try: await page.wait_for_selector("body", timeout=3000)
    except PWTimeout: pass

async def _scrape_impl(debug=False) -> pd.DataFrame:
    _ensure_debug_dirs()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage",
        ])
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"),
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            extra_http_headers={"Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8"},
            viewport={"width":1280,"height":900},
        )
        await context.route("**/*", lambda r: asyncio.create_task(_route_block(r)))
        page = await context.new_page()

        # 모든 JSON 수집
        payloads: List[Dict[str,Any]] = []
        page.on("response", lambda resp: asyncio.create_task(_collect_any_json(resp, payloads)))

        await _wait_dom_ready(page, BEST_URL)
        if os.getenv("OY_FORCE_KR","1") == "1":
            await _force_region_kr(page)
            await asyncio.sleep(1.0)

        # 가벼운 스크롤
        for _ in range(10):
            await page.evaluate("window.scrollBy(0, 1600)")
            await asyncio.sleep(0.35)

        html = await page.content()
        df_json = _harvest_from_json_payloads(payloads)
        df_dom  = await _harvest_from_dom(html)

        if debug:
            ts = int(time.time())
            with open(f"{DEBUG_DIR}/page_{ts}.html","w",encoding="utf-8") as f: f.write(html)
            if df_dom is not None and not df_dom.empty:
                df_dom.to_csv(f"{DEBUG_DIR}/parsed_dom_{ts}.csv", index=False, encoding="utf-8-sig")
            if df_json is not None and not df_json.empty:
                df_json.to_csv(f"{DEBUG_DIR}/parsed_json_{ts}.csv", index=False, encoding="utf-8-sig")

        await context.close(); await browser.close()

        df = df_json if (df_json is not None and not df_json.empty) else (df_dom if df_dom is not None else pd.DataFrame([]))
        if not df.empty:
            # 최종 컬럼 고정 (국내몰과 동일)
            want = ["rank","brand","name","original_price","sale_price","discount_pct","url","raw_name"]
            for c in want:
                if c not in df.columns: df[c] = pd.NA
            df = df[want].copy()
            df["rank"] = range(1, len(df)+1)
            for c in ["brand","name","url","raw_name"]:
                df[c] = df[c].fillna("").astype(str)
        return df

async def _collect_any_json(resp, acc: List[Dict[str, Any]]):
    try:
        if "application/json" in (resp.headers.get("content-type") or ""):
            data = await resp.json()
            acc.append({"url": resp.url, "data": data})
    except Exception: pass

def scrape_oy_global_us(debug=False) -> pd.DataFrame:
    return asyncio.run(_scrape_impl(debug=debug))
