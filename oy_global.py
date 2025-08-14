import asyncio
import re
from typing import List, Dict, Optional, Tuple

from playwright.async_api import async_playwright

BASE_URL = "https://global.oliveyoung.com"
BEST_URL = f"{BASE_URL}/category/best-seller"

PRICE_RE = re.compile(r"US?\$[\s]*([0-9]+(?:\.[0-9]+)?)")
NUM_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")

CATEGORY_NOISE = {
    "all","skincare","makeup","bath & body","hair","face masks","suncare",
    "k-pop","makeup brush & tools","wellness","supplements","food & drink",
}
BADGE_NOISE = {"hot deal","best"}

def _to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = NUM_RE.search(text.replace(",", ""))
    return float(m.group(1)) if m else None

def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text.replace(",", ""))
    if m:
        return float(m.group(1))
    return _to_float(text)

async def _txt(loc) -> str:
    try:
        t = (await loc.inner_text()).strip()
        return re.sub(r"\s+", " ", t)
    except:
        return ""

async def _first_text(scope, sels: List[str]) -> str:
    for s in sels:
        loc = scope.locator(s).first
        try:
            if await loc.count() > 0:
                t = await _txt(loc)
                if t:
                    return t
        except:
            pass
    return ""

def _normalize_slack_line(brand: str, product: str) -> str:
    b = (brand or "").strip()
    p = (product or "").strip()
    if b and p.lower().startswith(b.lower()):
        return p
    if b and p:
        return f"{b} {p}"
    return p or b

def _is_noise_name(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return True
    if t in BADGE_NOISE:
        return True
    # 너무 짧은(라벨) 텍스트 제거
    if len(t) <= 2:
        return True
    return False

def _is_noise_brand(brand: str) -> bool:
    b = (brand or "").strip().lower()
    if not b:
        return False
    return b in CATEGORY_NOISE or b in BADGE_NOISE

async def _scroll_until(page, locator, need=100, max_loops=80):
    for _ in range(max_loops):
        cnt = await locator.count()
        if cnt >= need:
            return
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await page.wait_for_timeout(900)

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()
        await page.goto(BEST_URL, wait_until="domcontentloaded")

        # 상품 링크가 하나라도 보일 때까지 대기
        await page.wait_for_selector("a[href*='product/detail']", timeout=30000)

        all_links = page.locator("a[href*='product/detail']")
        await _scroll_until(page, all_links, need=140, max_loops=80)

        # 트렌딩 섹션 헤더의 y 좌표(경계) 구하기 — 없으면 매우 큰 값으로.
        trending_y = 10**9
        for patt in [
            r"What's trending in Korea", r"What’s trending in Korea",
            r"Trending in Korea", r"트렌딩", r"요즘 한국"
        ]:
            tr = page.locator(f"text=/{patt}/i").first
            if await tr.count() > 0:
                bb = await tr.bounding_box()
                if bb and bb.get("y") is not None:
                    trending_y = float(bb["y"])
                    break

        handles = await all_links.element_handles()

        # 링크의 위치로 정렬(읽기 순)
        async def _pos(h) -> Tuple[float, float]:
            bb = await h.bounding_box()
            if not bb:
                return (10**9, 10**9)
            return (bb.get("y") or 10**9, bb.get("x") or 10**9)

        handles = sorted(
            [h for h in handles],
            key=lambda h: asyncio.get_event_loop().run_until_complete(_pos(h))
        )

        items: List[Dict] = []
        seen = set()
        rank = 1

        for a in handles:
            bb = await a.bounding_box() or {}
            y = float(bb.get("y") or 10**9)

            # 트렌딩 섹션 아래로 내려가면 중단
            if y >= trending_y:
                break

            href = await a.get_attribute("href") or ""
            if "/product/detail" not in href:
                continue
            if href.startswith("http"):
                product_url = href
            elif href.startswith("/"):
                product_url = BASE_URL + href
            else:
                product_url = BASE_URL + "/" + href.lstrip("/")

            if product_url in seen:
                continue
            seen.add(product_url)

            # 카드 래퍼
            card = await a.evaluate_handle(
                "node => node.closest('li, .prd, .product, .card, .product-item, .item') || node"
            )

            brand = await _first_text(
                card,
                [".brand",".prd_brand",".product-brand",".txt_brand","strong.brand","span.brand"]
            )
            name = await _first_text(
                card,
                [".name",".prd_name",".product-name",".txt_name","a.name","a.product-name"]
            )
            if not name:
                # 링크 텍스트/이미지 대체
                name = await _txt(a)
            if not name:
                name = await _first_text(card, ["img[alt]"])

            # 라벨/카테고리 노이즈 제거
            if _is_noise_name(name) or _is_noise_brand(brand):
                continue

            # 가격들
            cur_txt = await _first_text(
                card, [".price .sale",".price .current",".price .now",".product-price-now",".product-price"]
            ) or await _first_text(card, [".price",".prd_price",".txt_price"])
            org_txt = await _first_text(
                card, [".price del",".price .original",".product-price-was",".origin","del"]
            )
            val_txt = await _first_text(card, [".value",".benefit",".txt_value",".value-price"])
            pct_txt = await _first_text(card, [".discount",".sale-percent",".percent",".pct"])

            price_current = _parse_price(cur_txt)
            price_original = _parse_price(org_txt)
            value_price = _parse_price(val_txt)
            has_value_price = bool(value_price is not None)

            discount_pct = _to_float(pct_txt)
            if discount_pct is None and price_current and price_original:
                try:
                    discount_pct = round((1 - (price_current / price_original)) * 100, 2)
                except ZeroDivisionError:
                    discount_pct = None

            # 이미지
            img = await _first_text(card, ["img[src]","img[data-src]"])
            if img and not img.startswith("http"):
                img = BASE_URL + "/" + img.lstrip("/")

            # 가격이 전혀 없거나 이름이 비정상이면 스킵
            if not name or (price_current is None and price_original is None):
                continue

            items.append({
                "date_kst": None,
                "rank": rank,
                "brand": (brand or "").strip(),
                "product_name": (name or "").strip(),
                "price_current_usd": price_current,
                "price_original_usd": price_original,
                "discount_rate_pct": discount_pct,
                "value_price_usd": value_price,
                "has_value_price": "TRUE" if has_value_price else "FALSE",
                "product_url": product_url,
                "image_url": img,
            })

            rank += 1
            if rank > 100:
                break

        # 날짜 채우기
        from datetime import datetime, timezone, timedelta
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).strftime("%Y-%m-%d")
        for it in items:
            it["date_kst"] = today

        return items

def build_top10_slack_text(df) -> str:
    lines = []
    header = f"*올리브영 글로벌 전체 랭킹* ({df.iloc[0]['date_kst']} KST)\n*TOP 10*"
    lines.append(header)

    for _, row in df.iterrows():
        rank = int(row["rank"])
        brand = (row["brand"] or "").strip()
        name = (row["product_name"] or "").strip()
        cur_p = row.get("price_current_usd")
        org_p = row.get("price_original_usd")
        pct = row.get("discount_rate_pct")
        url = row.get("product_url")

        title = _normalize_slack_line(brand, name)
        link = f"<{url}|{title}>"

        tail = []
        if cur_p is not None:
            tail.append(f"US${cur_p:.2f}")
        if org_p is not None:
            tail.append(f"(정가 US${org_p:.2f})")
        if pct is not None:
            tail.append(f"(↓{pct:.2f}%)")

        line = f"{rank}. {link} – " + " ".join(tail) if tail else f"{rank}. {link}"
        lines.append(line)

    return "\n".join(lines)
