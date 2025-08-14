# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, Page, Locator

# -------------------------------------------------
# 기본 상수 / 셀렉터
# -------------------------------------------------
BASE_URL = "https://global.oliveyoung.com/"

# 페이지 내 섹션 제목(h2/h3/tabs) 중 다음 정규식과 매칭되는 섹션만 사용
SECTION_TITLE_RE = re.compile(r"(Top Orders|Best Sellers)", re.I)

# 실제 상품 카드 앵커 (배너/광고가 아닌 상품 상세로 가는 링크만)
CARD_ANCHOR = "a[href*='product/detail']"

# 가격 추출용
USD_RE = re.compile(r"US\$\s*([0-9]+(?:\.[0-9]+)?)")
VALUE_RE = re.compile(r"(?:Value|정가)\s*[: ]?\s*US\$\s*([0-9]+(?:\.[0-9]+)?)", re.I)

# 한국 시간
KST = timezone(timedelta(hours=9))


# -------------------------------------------------
# 유틸리티
# -------------------------------------------------
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


# -------------------------------------------------
# 섹션/탭 활성화 & 카드 수집
# -------------------------------------------------
async def _activate_top_orders_tab(page: Page) -> None:
    """
    탭 UI일 경우 'Top Orders' / 'Best Sellers' 탭을 클릭해서 활성화한다.
    실패해도 무시(섹션이 바로 보이는 레이아웃일 수 있음).
    """
    try:
        # 탭 후보: tab-swiper-title, tab, button 등
        tab_candidates = page.locator(
            ".tab-swiper-title, .tab, button, [role='tab']"
        ).filter(has_text=SECTION_TITLE_RE)
        if await tab_candidates.count() > 0:
            tab = tab_candidates.first
            # attached 상태만 보장하고 클릭 시도
            await tab.wait_for(state="attached", timeout=10000)
            await tab.click(timeout=10000)
            # 탭 콘텐츠 렌더링 여유
            await page.wait_for_timeout(500)
    except Exception:
        pass


async def _get_section_locator(page: Page, title_re: re.Pattern) -> Locator:
    """
    페이지에서 h2/h3 텍스트가 title_re와 매칭되는 섹션(또는 가장 가까운 상위 컨테이너)을 반환.
    - visible을 기다리지 않고 attached로만 대기
    - 실패 시 텍스트 검색으로 폴백
    """
    # 우선 탭을 활성화(있으면)
    await _activate_top_orders_tab(page)

    # 헤더들을 attached 기준으로 대기
    await page.wait_for_selector("h2, h3", state="attached", timeout=40000)
    headings = page.locator("h2, h3")
    count = await headings.count()

    for i in range(count):
        h = headings.nth(i)
        # visible을 강제하지 않고 텍스트만 확보
        txt = ""
        try:
            txt = (await h.inner_text()).strip()
        except Exception:
            continue
        if not txt:
            continue
        if title_re.search(txt):
            # 가장 가까운 section/div 조상을 섹션으로 잡아 스코프 한정
            section = h.locator("xpath=ancestor::*[self::section or self::div][1]")
            return section

    # 폴백: 아무 헤딩도 못 잡았으면 페이지 텍스트 매칭 시도
    try:
        any_text = page.get_by_text(title_re)
        if await any_text.count() > 0:
            h = any_text.first
            section = h.locator("xpath=ancestor::*[self::section or self::div][1]")
            return section
    except Exception:
        pass

    raise RuntimeError("Top Orders/Best Sellers 섹션을 찾을 수 없습니다.")


async def _extract_cards_in_top_orders(page: Page) -> Locator:
    """
    Top Orders 섹션 안의 실제 상품 카드 앵커만 Locator로 반환.
    """
    section = await _get_section_locator(page, SECTION_TITLE_RE)
    cards = section.locator(CARD_ANCHOR)
    # 첫 카드가 붙을 때까지 대기 (가시성보단 attached가 안전)
    await cards.first.wait_for(state="attached", timeout=30000)
    return cards, section


async def _autoscroll_section(page: Page, section: Locator, target_count: int = 100) -> None:
    """
    섹션 내부를 부드럽게 스크롤해서 lazy-load된 카드가 충분히 로드되도록 함.
    """
    container = section
    try:
        # 섹션 내부 스크롤 우선
        scroll_box = section.locator(
            "xpath=ancestor-or-self::*[contains(@style,'overflow') or contains(@class,'scroll')][1]"
        )
        if await scroll_box.count() > 0:
            container = scroll_box.first
    except Exception:
        pass

    prev_height = -1
    same_count = 0
    for _ in range(40):  # 충분할 만큼만
        # 현재 카드 개수 확인
        try:
            cards = section.locator(CARD_ANCHOR)
            n = await cards.count()
            if n >= target_count:
                break
        except Exception:
            n = 0

        # 스크롤
        try:
            await container.evaluate(
                "(el) => el.scrollTo({top: el.scrollHeight, behavior: 'smooth'})"
            )
        except Exception:
            # 섹션 스크롤이 안 되면 전체 페이지 스크롤
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)

        # 높이 변화 체크
        try:
            cur_h = await container.evaluate("el => el.scrollHeight")
        except Exception:
            cur_h = await page.evaluate("() => document.body.scrollHeight")

        if cur_h == prev_height:
            same_count += 1
        else:
            same_count = 0
            prev_height = cur_h

        if same_count >= 5:
            break


# -------------------------------------------------
# 가격 파싱
# -------------------------------------------------
def _parse_prices_from_text(text: str) -> Dict[str, Optional[float]]:
    """
    카드 전체 텍스트에서 가격들을 추출.
    - 현재가: 가장 작은 USD 값으로 추정
    - 정가(원가): '정가' 또는 'Value' 문구가 붙은 값이 있으면 그 값 우선
                 없으면 현재가보다 큰 값들 중 최소값을 원가로 추정
    - value_price_usd: 'Value' 표기 값 있으면 채움
    """
    numbers = [float(x) for x in USD_RE.findall(text)]
    value_price = _to_float(next(iter(VALUE_RE.findall(text)), None))

    current = None
    original = None

    if numbers:
        # 현재가는 가장 작은 값으로 추정
        current = min(numbers)

        # 원가 후보들 (현재가보다 큰 값)
        bigger = sorted([x for x in numbers if x > current])
        if value_price is not None and value_price in bigger:
            # value는 구성 총액일 수 있어 우선 제외
            bigger_wo_value = [x for x in bigger if x != value_price]
            if bigger_wo_value:
                original = bigger_wo_value[0]
            else:
                original = value_price
        else:
            if bigger:
                original = bigger[0]

    return {
        "price_current_usd": _round2(current),
        "price_original_usd": _round2(original),
        "value_price_usd": _round2(value_price),
        "has_value_price": value_price is not None,
    }


# -------------------------------------------------
# 상품 정보 파싱
# -------------------------------------------------
async def _extract_name_and_brand(card: Locator) -> Dict[str, str]:
    """
    카드에서 브랜드/상품명을 추출.
    - 전용 셀렉터 우선
    - 폴백: 앵커 텍스트 라인 분리하여 [브랜드, 상품명] 패턴 시도
    """
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
        # 폴백: 앵커 전체 텍스트 분석
        try:
            t = (await card.inner_text()).strip()
            lines = [l.strip() for l in t.splitlines() if l.strip()]
            if not brand and len(lines) >= 2:
                brand = lines[0]
            if not name:
                if len(lines) >= 2:
                    name = lines[1]
                elif lines:
                    name = lines[0]
        except Exception:
            pass

    if brand and len(brand) > 60:
        brand = ""

    return {"brand": brand, "product_name": name}


async def _extract_image(card: Locator) -> Optional[str]:
    try:
        img = card.locator("img").first
        if await img.count() == 0:
            return None
        src = await img.get_attribute("src")
        if not src:
            src = await img.get_attribute("data-src")
        return _abs_url(src)
    except Exception:
        return None


# -------------------------------------------------
# 메인 스크레이퍼
# -------------------------------------------------
async def scrape_oliveyoung_global() -> List[Dict]:
    """
    Top Orders/Best Sellers 섹션에서 최대 100개 상품을 수집하여 dict 리스트로 반환.
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
        # 안정 대기
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # Top Orders 섹션 카드 추출
        cards, section = await _extract_cards_in_top_orders(page)

        # 필요시 섹션 오토스크롤
        await _autoscroll_section(page, section, target_count=100)

        count = await cards.count()
        count = min(count, 100)

        for i in range(count):
            card = cards.nth(i)

            # URL / 이미지
            href = await card.get_attribute("href")
            product_url = _abs_url(href)
            image_url = await _extract_image(card)

            # 브랜드/상품명
            nm = await _extract_name_and_brand(card)
            brand = nm.get("brand", "")
            product_name = nm.get("product_name", "")

            # 가격
            text = (await card.inner_text()) or ""
            price_info = _parse_prices_from_text(text)
            cur = price_info["price_current_usd"]
            orig = price_info["price_original_usd"]
            disc_pct = None
            if cur is not None and orig and orig > 0:
                disc_pct = _round2((1 - cur / orig) * 100.0)

            item = {
                "date_kst": now_kst,
                "rank": i + 1,
                "brand": brand,
                "product_name": product_name,
                "price_current_usd": cur or 0,
                "price_original_usd": orig or 0,
                "discount_rate_pct": disc_pct or 0,
                "value_price_usd": price_info["value_price_usd"] or 0,
                "has_value_price": price_info["has_value_price"],
                "product_url": product_url or "",
                "image_url": image_url or "",
            }
            items.append(item)

        await context.close()
        await browser.close()

    return items


# -------------------------------------------------
# 디버그 실행용 (로컬 테스트 시)
# -------------------------------------------------
if __name__ == "__main__":
    async def _debug():
        data = await scrape_oliveyoung_global()
        for r in data[:10]:
            print(r)

    asyncio.run(_debug())
