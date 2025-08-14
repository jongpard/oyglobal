# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import asyncio
from typing import Dict, List, Tuple
from datetime import datetime, timezone, timedelta

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com"

# 서울(KST) 타임스탬프
KST = timezone(timedelta(hours=9))


# -------------------- 공통 유틸 --------------------
_PRICE_RE = re.compile(r"US\$\s*([\d]+(?:\.\d{1,2})?)")

def _parse_prices_from_text(text: str) -> Tuple[float, float]:
    """
    카드 전체 텍스트에서 US$ 가격들을 찾아
    (현재가, 정가)를 추정한다.
    - 보통 현재가가 더 작으므로 min=현재가, max=정가로 처리
    - 둘 중 하나만 나오면 둘 다 동일한 값으로 세팅
    """
    nums = [float(x) for x in _PRICE_RE.findall(text)]
    if not nums:
        return 0.0, 0.0
    if len(nums) == 1:
        return nums[0], nums[0]
    cur, orig = min(nums), max(nums)
    return cur, orig


def _round_pct(cur: float, orig: float) -> int:
    if orig <= 0:
        return 0
    pct = 100.0 * (1.0 - (cur / orig))
    # 반올림하여 정수로
    return int(round(max(0.0, pct)))


async def _autoscroll(page: Page, need: int = 100) -> None:
    """
    게으른 로딩을 고려해 부드럽게 스크롤 다운.
    """
    last = 0
    same = 0
    for _ in range(80):  # 충분히 스크롤
        await page.mouse.wheel(0, 1600)
        await page.wait_for_timeout(300)
        # 앵커 수로 로딩 상황 대략 파악
        count = await page.locator("a[href*='/product/detail']").count()
        if count >= need:
            break
        if count == last:
            same += 1
        else:
            same = 0
        last = count
        if same >= 8:  # 더 이상 늘지 않으면 종료
            break


async def _find_top_orders_section(page: Page) -> Locator:
    """
    'Top Orders' 섹션 래퍼를 찾아 반환.
    1순위: 'Top Orders|Best Sellers|TOP 100' 헤딩
    실패 시: 카테고리 칩(Skincare, Makeup, Bath & Body...)이 포함된 섹션 중
             상품 카드(a[href*='/product/detail'])가 가장 많은 섹션.
    """
    # 1) 명시적 헤딩 탐색
    heading = page.locator("h2, h3").filter(
        has_text=re.compile(r"(Top\s*Orders|Best'?s?\s*Sellers|TOP\s*100)", re.I)
    )
    if await heading.count():
        return heading.first.locator("xpath=ancestor::*[self::section or self::div][1]")

    # 2) 카테고리 칩이 있는 섹션 후보
    chips = r"(Skincare|Makeup|Bath\s*&\s*Body|Hair|Face\s*Masks|Suncare|K-?Pop|Wellness|Supplements|Food\s*&\s*Drink)"
    candidates = page.locator("section, div").filter(has_text=re.compile(chips, re.I))

    best = None
    best_links = -1
    n = await candidates.count()
    for i in range(n):
        sec = candidates.nth(i)
        links = await sec.locator("a[href*='/product/detail']").count()
        # 'What's trending' 제외(해당 문구가 있으면 제외)
        has_trending = await sec.locator(":text('What\\'s trending in Korea')").count()
        if not has_trending and links > best_links:
            best, best_links = sec, links

    if best:
        return best

    # 3) 마지막 폴백: 전체 중 제품 링크가 가장 많은 섹션
    all_containers = page.locator("section, div")
    best = None
    best_links = -1
    n = await all_containers.count()
    for i in range(min(n, 40)):
        sec = all_containers.nth(i)
        links = await sec.locator("a[href*='/product/detail']").count()
        if links > best_links:
            best, best_links = sec, links

    if not best:
        # 페이지 전체
        return page.locator("body")
    return best


async def _extract_cards_in_top_orders(section: Locator) -> List[Locator]:
    """
    섹션 내부에서 '상품 카드' 앵커 기준으로 상위 래퍼를 잡아 카드 리스트로 만든다.
    """
    # 앵커 → 가장 가까운 카드 래퍼로 승격
    anchors = section.locator("a[href*='/product/detail']")
    count = await anchors.count()
    cards: List[Locator] = []
    seen = set()
    for i in range(count):
        a = anchors.nth(i)
        # 카드 래퍼 후보
        card = a.locator(
            "xpath=ancestor::li[1] | xpath=ancestor::div[contains(@class,'prd')][1] | xpath=ancestor::div[1]"
        )
        # 고유키(첫 앵커 href)로 중복 제거
        href = await a.get_attribute("href")
        if not href:
            continue
        if href in seen:
            continue
        seen.add(href)
        cards.append(card)

    return cards


async def _read_text(el: Locator) -> str:
    try:
        txt = (await el.inner_text()).strip()
        return re.sub(r"\s+", " ", txt)
    except:
        return ""


async def _read_src(el: Locator) -> str:
    for attr in ("src", "data-src", "data-original", "srcset"):
        try:
            v = await el.get_attribute(attr)
            if v:
                # srcset일 때 첫 항목만
                return v.split()[0]
        except:
            pass
    return ""


async def scrape_oliveyoung_global() -> List[Dict]:
    """
    Top Orders 1~100 수집 (중복 제거, 정렬, 가격/할인 계산)
    반환: List[dict]
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)

        # 충분히 로드
        await _autoscroll(page, need=100)

        section = await _find_top_orders_section(page)
        # 섹션이 보이도록 한 번 더 스크롤
        await section.scroll_into_view_if_needed()
        await page.wait_for_timeout(400)

        # 섹션 내부만 추가 스크롤
        await _autoscroll(page, need=100)

        cards = await _extract_cards_in_top_orders(section)
        items: List[Dict] = []
        seen_href = set()

        for idx, card in enumerate(cards, start=1):
            if len(items) >= 100:
                break

            # 링크 / 이미지
            link = card.locator("a[href*='/product/detail']").first
            href = await link.get_attribute("href") or ""
            if not href or href in seen_href:
                continue
            seen_href.add(href)

            img = card.locator("img").first
            image_url = await _read_src(img)

            # 브랜드 / 제품명(카드 타이틀)
            # 사이트 구조가 종종 바뀌므로 다중 CSS 시도
            brand_sel = "[class*='brand'], .brand, .prd_brand, .brand-name"
            name_sel = "[class*='name'], .prd_name, .product-name, .name, .tit, .txt"

            brand = (await _read_text(card.locator(brand_sel).first)) or ""
            title = (await _read_text(card.locator(name_sel).first)) or ""
            if not title:
                # 앵커 텍스트 폴백
                title = await _read_text(link)

            # 가격 추정
            txt_all = await _read_text(card)
            price_current_usd, price_original_usd = _parse_prices_from_text(txt_all)
            discount_rate_pct = _round_pct(price_current_usd, price_original_usd)
            has_value_price = int(price_original_usd > price_current_usd)

            items.append(
                {
                    "rank": idx,
                    "brand": brand,
                    "product_name": title,
                    "price_current_usd": price_current_usd,
                    "price_original_usd": price_original_usd,
                    "discount_rate_pct": discount_rate_pct,
                    "has_value_price": bool(has_value_price),
                    "product_url": href if href.startswith("http") else (BASE_URL + href),
                    "image_url": image_url,
                }
            )

        await browser.close()

    # 랭크 재정렬 & 100개 제한
    items = items[:100]
    for i, row in enumerate(items, 1):
        row["rank"] = i

    return items
