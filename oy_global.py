# oy_global.py
# Olive Young Global Top Orders (Best Sellers) scraper
# - Top Orders 영역만 1~100위 수집
# - CSV 저장 전 brand 컬럼 정규화(브랜드명만 남김)

import asyncio
import math
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import pandas as pd
from playwright.async_api import async_playwright, Page


BASE_URL = "https://global.oliveyoung.com/"
DATA_DIR = "data"
MAX_RANK = int(os.getenv("MAX_RANK", "100"))  # 100위까지
SCROLL_PAUSE = 300  # ms


# ---------- utils ----------
def kst_today_str() -> str:
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime("%Y-%m-%d")


def _parse_price_usd(text: str) -> float:
    """문자열에서 US$ 금액 추출 -> float (없으면 NaN)"""
    if not text:
        return float("nan")
    m = re.search(r"US\$ ?([0-9][0-9,]*\.?[0-9]*)", text, flags=re.I)
    return float(m.group(1).replace(",", "")) if m else float("nan")


def _clean_spaces(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s


def _clean_brand_value(brand: str, product_name: str) -> str:
    """
    brand 칸 정리:
    - 여러 줄이면 첫 번째 비어있지 않은 줄만 사용
    - product_name 섞여 있으면 제거
    - 공백/개행 정리
    """
    if brand is None:
        return ""
    text = str(brand)

    # 1) 첫 줄만
    first_non_empty = ""
    for part in text.splitlines():
        part = part.strip()
        if part:
            first_non_empty = part
            break
    text = first_non_empty or text.strip()

    # 2) product_name이 포함되어 있으면 제거
    if isinstance(product_name, str) and product_name:
        pn = re.sub(r"\s+", " ", product_name).strip()
        tt = re.sub(r"\s+", " ", text).strip()
        if pn and pn.lower() in tt.lower():
            pattern = re.compile(re.escape(pn), flags=re.I)
            tt = pattern.sub("", tt).strip()
            if tt:
                text = tt

    # 3) 공백 정리
    return _clean_spaces(text)


async def _ensure_top_orders_only(page: Page) -> None:
    """
    첫 로딩 안정화 + Top Orders 로드 유도 스크롤.
    'What's trending in Korea?' 헤더가 보이면 그 지점까지만 로드했다고 판단.
    """
    # 초기 로드 안정화
    await page.wait_for_load_state("domcontentloaded")
    # 네트워크가 잠잠해질 때 한 번 더 대기 (간헐적 레이지 로드 대응)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    # 요소가 DOM에 붙기만 하면 통과(visibility 요구 X)
    try:
        await page.wait_for_selector("a[href*='product']", state="attached", timeout=60000)
    except Exception:
        # 그래도 실패하면 스크롤을 조금 내려보며 재시도
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 800)")
            await page.wait_for_timeout(500)
            if await page.locator("a[href*='product']").count():
                break

    # 스크롤로 Top Orders 영역 로드
    for _ in range(40):
        trending = page.locator("text=/what\\'s trending in korea\\?/i")
        if await trending.first.is_visible():
            break
        await page.evaluate("window.scrollBy(0, 1200)")
        await page.wait_for_timeout(SCROLL_PAUSE)


async def _extract_cards_in_top_orders(page: Page):
    """
    Top Orders(=Best Sellers) 그리드 내 카드 컨테이너들을 반환
    - 'What's trending in Korea?' 이전 영역의 카드만 수집
    """
    # Top Orders/Best Sellers 헤더 기준으로 영역 잡기
    top_area = None
    for sel in [
        "section:has(h2:has-text('Top Orders'))",
        "section:has(h2:has-text('Best Sellers'))",
        "div:has(> h2:has-text('Top Orders'))",
        "div:has(> h2:has-text('Best Sellers'))",
    ]:
        loc = page.locator(sel)
        if await loc.count():
            top_area = loc.first
            break

    # 못 찾으면 화면 상단부터 트렌딩 헤더 전까지 사용
    if not top_area:
        trending = page.locator("text=/what\\'s trending in korea\\?/i")
        trending_top = await trending.bounding_box()
        cutoff_y = trending_top["y"] if trending_top else math.inf

        cards = []
        all_cards = page.locator("a[href*='product']").locator("..")  # 카드 컨테이너(앵커의 부모)
        count = await all_cards.count()
        for i in range(count):
            c = all_cards.nth(i)
            box = await c.bounding_box()
            if box and box["y"] < cutoff_y:
                cards.append(c)
        return cards

    # top_area 내부의 카드만
    cards = top_area.locator("a[href*='product']").locator("..")
    arr = [cards.nth(i) for i in range(await cards.count())]
    return arr


async def _card_to_item(card, rank: int) -> Dict:
    """개별 카드에서 필요한 정보 추출"""
    # 브랜드/상품명
    brand_txt = await card.locator("text").first.inner_text()
    brand = ""
    name = ""

    # 1) 자주 쓰는 구조
    for b_sel in [".brand", ".prd_brand", "strong.brand", "span.brand"]:
        if await card.locator(b_sel).count():
            brand = await card.locator(b_sel).first.inner_text()
            break
    for n_sel in [".name", ".prd_name", ".title", "strong.name", "a.name"]:
        if await card.locator(n_sel).count():
            name = await card.locator(n_sel).first.inner_text()
            break

    # 2) 보조 추론
    if not brand or not name:
        lines = [ln.strip() for ln in brand_txt.splitlines() if ln.strip()]
        if not brand and lines:
            brand = lines[0]
        if not name and len(lines) >= 2:
            name = lines[1]

    brand = _clean_spaces(brand)
    name = _clean_spaces(name)

    # 현재가/원가/밸류
    whole_text = await card.inner_text()
    # Value: US$xx.xx
    value_price_usd = _parse_price_usd(re.search(r"Value[:：]\s*US\$ ?[0-9,\.]+", whole_text, flags=re.I).group(0)) \
        if re.search(r"Value[:：]\s*US\$ ?[0-9,\.]+", whole_text, flags=re.I) else float("nan")
    has_value_price = not math.isnan(value_price_usd)

    orig = float("nan")
    for o_sel in ["s:has-text('US$')", "del:has-text('US$')"]:
        if await card.locator(o_sel).count():
            try:
                o_text = await card.locator(o_sel).first.inner_text()
                orig = _parse_price_usd(o_text)
                break
            except Exception:
                pass

    cur = float("nan")
    price_node = None
    for p_sel in [
        "strong:has-text('US$')", ".price:has-text('US$')",
        "span:has-text('US$')", "p:has-text('US$')"
    ]:
        if await card.locator(p_sel).count():
            price_node = card.locator(p_sel).first
            break
    if price_node:
        cur = _parse_price_usd(await price_node.inner_text())

    # 카드 전체 텍스트에서 금액 추정
    if math.isnan(cur):
        prices = re.findall(r"US\$ ?([0-9][0-9,]*\.?[0-9]*)", whole_text)
        if prices:
            cur = float(prices[-1].replace(",", ""))
            if len(prices) >= 2 and math.isnan(orig):
                orig = float(prices[-2].replace(",", ""))

    price_original = orig
    if has_value_price and not math.isnan(value_price_usd):
        price_original = value_price_usd

    discount_rate_pct = float("nan")
    if (price_original and not math.isnan(price_original)) and (cur and not math.isnan(cur)) and price_original > 0:
        discount_rate_pct = round((1 - (cur / price_original)) * 100, 2)

    # 링크/이미지
    a = card.locator("a[href*='product']").first
    product_url = ""
    try:
        product_url = await a.get_attribute("href") or ""
    except Exception:
        pass
    if product_url and product_url.startswith("/"):
        product_url = BASE_URL.rstrip("/") + product_url

    image_url = ""
    for img_sel in ["img", "picture img", "img.prd_img"]:
        if await card.locator(img_sel).count():
            image_url = await card.locator(img_sel).first.get_attribute("src") or ""
            break

    brand = _clean_brand_value(brand, name)

    return {
        "date_kst": kst_today_str(),
        "rank": rank,
        "brand": brand,
        "product_name": name,
        "price_current_usd": cur,
        "price_original_usd": price_original,
        "discount_rate_pct": discount_rate_pct,
        "value_price_usd": value_price_usd if has_value_price else float("nan"),
        "has_value_price": bool(has_value_price),
        "product_url": product_url,
        "image_url": image_url,
    }


# ---------- public API ----------
async def scrape_oliveyoung_global() -> List[Dict]:
    """Top Orders 1~100위 수집 -> Dict 리스트 반환"""
    items: List[Dict] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(locale="en-US")
        page = await ctx.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded")

        await _ensure_top_orders_only(page)
        cards = await _extract_cards_in_top_orders(page)

        # 앞에서부터 100개만
        cards = cards[:MAX_RANK]

        rank = 1
        for c in cards:
            try:
                item = await _card_to_item(c, rank)
                items.append(item)
                rank += 1
            except Exception:
                continue

        await ctx.close()
        await browser.close()

    return items


def save_items_to_csv(items: List[Dict]) -> str:
    """items -> DataFrame -> brand 정규화 -> CSV 저장, 경로 반환"""
    os.makedirs(DATA_DIR, exist_ok=True)
    df = pd.DataFrame(items)

    # brand 컬럼 정규화 (브랜드명만 남김)
    if "brand" in df.columns and "product_name" in df.columns:
        df["brand"] = df.apply(
            lambda r: _clean_brand_value(r.get("brand", ""), r.get("product_name", "")),
            axis=1,
        )

    cols = [
        "date_kst", "rank", "brand", "product_name",
        "price_current_usd", "price_original_usd", "discount_rate_pct",
        "value_price_usd", "has_value_price",
        "product_url", "image_url",
    ]
    df = df.reindex(columns=cols)

    out_path = os.path.join(DATA_DIR, f"oliveyoung_global_{kst_today_str()}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path


# 로컬 테스트용
if __name__ == "__main__":
    async def _run():
        items = await scrape_oliveyoung_global()
        path = save_items_to_csv(items)
        print(f"Saved: {path}, rows={len(items)}")
    asyncio.run(_run())
