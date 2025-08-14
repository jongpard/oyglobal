# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com/"

# 상품 상세로 가는 앵커(배너/프로모도 일부 포함될 수 있으나 텍스트/좌표로 후필터링)
CARD_ANCHOR = "a[href*='product/detail']"

# 가격 추출
USD_RE = re.compile(r"US\$\s*([0-9]+(?:\.[0-9]+)?)")
VALUE_RE = re.compile(r"(?:Value|정가)\s*[: ]?\s*US\$\s*([0-9]+(?:\.[0-9]+)?)", re.I)

# 경계 헤더(트렌딩 시작 지점) – 다국어 대비
TRENDING_RE = re.compile(r"(what(?:'|’)?s trending in korea|트렌딩|요즘.*코리아)", re.I)

KST = timezone(timedelta(hours=9))


def _to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _round2(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), 2)
    except Exception:
        return None


def _abs_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(BASE_URL, href)


def _parse_prices_from_text(text: str) -> Dict[str, Optional[float]]:
    numbers = [float(x) for x in USD_RE.findall(text or "")]
    value_price = _to_float(next(iter(VALUE_RE.findall(text or "")), None))

    current = None
    original = None

    if numbers:
        current = min(numbers)
        bigger = sorted([x for x in numbers if x > (current or 0.0)])

        if value_price is not None and value_price in bigger:
            bigger_wo_value = [x for x in bigger if x != value_price]
            original = bigger_wo_value[0] if bigger_wo_value else value_price
        else:
            original = bigger[0] if bigger else None

    disc = None
    if current is not None and original and original > 0:
        disc = _round2((1 - current / original) * 100.0)

    return {
        "price_current_usd": _round2(current),
        "price_original_usd": _round2(original),
        "discount_rate_pct": disc,
        "value_price_usd": _round2(value_price),
        "has_value_price": value_price is not None,
    }


async def _extract_name_and_brand(card: Locator) -> Dict[str, str]:
    brand = ""
    name = ""

    brand_candidates = [
        "[class*='brand']",
        ".brand",
        ".prd_brand",
        "[data-role='brand']",
    ]
    name_candidates = [
        "[class*='name']",
        ".name",
        ".prd_name",
        "[data-role='name']",
        "[class*='title']",
        ".title",
    ]

    for sel in brand_candidates:
        try:
            el = card.locator(sel).first
            if await el.count() > 0:
                txt = (await el.inner_text()).strip()
                if txt:
                    brand = txt
                    break
        except Exception:
            pass

    for sel in name_candidates:
        try:
            el = card.locator(sel).first
            if await el.count() > 0:
                txt = (await el.inner_text()).strip()
                if txt:
                    name = txt
                    break
        except Exception:
            pass

    if not (brand and name):
        try:
            t = (await card.inner_text()).strip()
            lines = [l.strip() for l in t.splitlines() if l.strip()]
            if not brand and len(lines) >= 2:
                brand = lines[0]
            if not name:
                name = lines[1] if len(lines) >= 2 else (lines[0] if lines else "")
        except Exception:
            pass

    if brand and len(brand) > 60:
        brand = ""

    return {"brand": brand, "product_name": name}


async def _get_trending_boundary_y(page: Page) -> Optional[float]:
    """
    'What's trending in Korea' 섹션 헤더의 Y 좌표를 반환. 없으면 None.
    """
    try:
        hdr = page.get_by_text(TRENDING_RE).first
        await hdr.wait_for(state="attached", timeout=5000)
        bb = await hdr.bounding_box()
        return bb["y"] if bb else None
    except Exception:
        return None


async def _autoscroll_page(page: Page, need: int = 100) -> None:
    """
    전체 페이지 스크롤로 lazy-load 로드. 고정 횟수 + 수렴체크.
    """
    prev_h = -1
    same = 0
    for _ in range(60):
        await page.evaluate("window.scrollBy(0, document.documentElement.clientHeight*0.9)")
        await page.wait_for_timeout(400)
        cur = await page.evaluate("() => document.body.scrollHeight")
        if cur == prev_h:
            same += 1
        else:
            same = 0
            prev_h = cur
        if same >= 5:
            break


async def scrape_oliveyoung_global() -> List[Dict]:
    """
    Top Orders(첫 메인 그리드)에서 최대 100개 수집.
    - 전역 카드 앵커를 수집하되, '트렌딩' 헤더의 Y좌표 위에 있는 카드들만 유지
    - 가격 텍스트(US$)가 있는 카드만 유지
    """
    items: List[Dict] = []
    now_kst = datetime.now(KST).date().isoformat()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 전역 카드가 붙기만 하면 진행
        await page.wait_for_selector(CARD_ANCHOR, state="attached", timeout=60000)

        # 경계 Y (트렌딩 헤더)
        boundary_y = await _get_trending_boundary_y(page)

        # 스크롤로 충분히 로드
        await _autoscroll_page(page, need=100)

        # 카드 후보
        cards = page.locator(CARD_ANCHOR)
        n = await cards.count()

        valid_indexes: List[int] = []
        for i in range(n):
            a = cards.nth(i)
            try:
                text = (await a.inner_text()) or ""
            except Exception:
                text = ""
            if "US$" not in text:
                continue  # 가격 없는 배너/프로모 제외

            if boundary_y is not None:
                try:
                    bb = await a.bounding_box()
                except Exception:
                    bb = None
                if not bb:
                    continue
                if bb["y"] >= boundary_y:
                    # 트렌딩 섹션 이하면 컷
                    continue

            valid_indexes.append(i)
            if len(valid_indexes) >= 100:
                break

        # valid 부족하면(경계 못찾음 등) 텍스트로만 필터한 상위 100개 사용
        if len(valid_indexes) < 10:
            valid_indexes = []
            for i in range(min(n, 200)):
                a = cards.nth(i)
                try:
                    text = (await a.inner_text()) or ""
                except Exception:
                    text = ""
                if "US$" in text:
                    valid_indexes.append(i)
                if len(valid_indexes) >= 100:
                    break

        rank = 1
        for idx in valid_indexes[:100]:
            card = cards.nth(idx)

            href = await card.get_attribute("href")
            product_url = _abs_url(href)

            # 이미지
            image_url = ""
            try:
                img = card.locator("img").first
                if await img.count() > 0:
                    src = await img.get_attribute("src") or await img.get_attribute("data-src")
                    image_url = _abs_url(src) or ""
            except Exception:
                pass

            # 이름/브랜드
            nm = await _extract_name_and_brand(card)
            brand = nm.get("brand", "")
            product_name = nm.get("product_name", "")

            # 가격/할인
            text = (await card.inner_text()) or ""
            p = _parse_prices_from_text(text)

            item = {
                "date_kst": now_kst,
                "rank": rank,
                "brand": brand,
                "product_name": product_name,
                "price_current_usd": p["price_current_usd"] or 0,
                "price_original_usd": p["price_original_usd"] or 0,
                "discount_rate_pct": p["discount_rate_pct"] or 0,
                "value_price_usd": p["value_price_usd"] or 0,
                "has_value_price": p["has_value_price"],
                "product_url": product_url or "",
                "image_url": image_url or "",
            }
            items.append(item)
            rank += 1

        await context.close()
        await browser.close()

    return items


# 로컬 테스트용
if __name__ == "__main__":
    async def _dbg():
        rows = await scrape_oliveyoung_global()
        print(len(rows))
        for r in rows[:5]:
            print(r)
    asyncio.run(_dbg())
