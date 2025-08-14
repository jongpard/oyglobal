# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com/"

# Top Orders 섹션 헤더 탐지 (여러 변형 커버)
SECTION_TITLE_RE = re.compile(
    r"(top\s*orders?|best\s*sellers?|top\s*100|top\s*50|top\s*10)",
    re.IGNORECASE,
)

# prdtNo 추출
PRDTNO_RE = re.compile(r"prdtNo=([A-Za-z0-9]+)")

# 시간대: 한국시간
KST = timezone(timedelta(hours=9))


# ---------- 공통 유틸 ----------

def _now_kst_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


async def _wait_until_idle(page: Page, sleep_ms: int = 250) -> None:
    # 네트워크가 아주 바쁘진 않아서, 살짝 쉰 뒤 DOM 안정화
    await page.wait_for_timeout(sleep_ms)


async def _autoscroll_until(page: Page, stop_if: callable, max_iter: int = 40) -> None:
    """페이지를 아래로 조금씩 스크롤하며 stop_if()가 True면 종료."""
    for _ in range(max_iter):
        if await stop_if():
            return
        await page.mouse.wheel(0, 1400)
        await _wait_until_idle(page, 300)


def _absolutize(href: str) -> str:
    if href.startswith("http"):
        return href
    return BASE_URL.rstrip("/") + "/" + href.lstrip("/")


def _round_discount(current: Optional[float], original: Optional[float]) -> int:
    if not current or not original or original <= 0:
        return 0
    return int(round((1 - (current / original)) * 100, 0))


def _clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


# ---------- 섹션/카드 탐지 ----------

async def _get_top_orders_container(page: Page) -> Locator:
    """
    h2/h3 헤더에서 Top Orders(또는 유사표기)를 찾아
    가장 가까운 섹션/컨테이너를 반환.
    """
    heading = page.locator("h2, h3").filter(has_text=SECTION_TITLE_RE).first
    await heading.wait_for(state="visible", timeout=30_000)

    # heading의 가장 가까운 section/div 조상 컨테이너
    container = heading.locator("xpath=ancestor::*[self::section or self::div][1]")
    await container.wait_for(state="visible", timeout=10_000)
    return container


async def _collect_unique_anchors(container: Locator, need: int = 100) -> List[Tuple[str, Locator]]:
    """
    컨테이너 내부의 a[href*='product/detail']들을 모으되,
    prdtNo 기준으로 중복 제거하여 최대 need개까지 반환.
    """
    seen: set[str] = set()
    items: List[Tuple[str, Locator]] = []

    async def snapshot() -> int:
        anchors = container.locator("a[href*='product/detail']").all()
        # locator.list()는 Playwright v1.46+; 현재 환경은 .all()로 충분
        for a in await anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            m = PRDTNO_RE.search(href)
            if not m:
                continue
            pid = m.group(1)
            if pid in seen:
                continue
            seen.add(pid)
            items.append((pid, a))
        return len(items)

    # 최초 수집
    await snapshot()

    # 스크롤하며 추가 수집
    async def stop_if() -> bool:
        await snapshot()
        return len(items) >= need

    # 컨테이너 내부에서만 스크롤이 안 될 수 있으므로 페이지 전체 스크롤
    page = container.page
    await _autoscroll_until(page, stop_if=stop_if, max_iter=60)
    # 마지막 한 번 더
    await snapshot()

    # 최대 need개로 자르기
    return items[:need]


# ---------- 카드 파싱 ----------

async def _closest_card(anchor: Locator) -> Locator:
    """
    앵커에서 가장 가까운 카드(li/div) 노드를 찾음.
    사이트 마크업 변동을 대비해 넓게 커버.
    """
    # li가 있으면 우선
    li = anchor.locator("xpath=ancestor::li[1]")
    if await li.count():
        return li.first
    # div 카드
    div = anchor.locator(
        "xpath=ancestor::div[contains(@class,'prd') or contains(@class,'product') or contains(@class,'item')][1]"
    )
    if await div.count():
        return div.first
    # 최후의 수단: 가장 가까운 div
    return anchor.locator("xpath=ancestor::div[1]")


async def _extract_prices(card: Locator) -> Tuple[Optional[float], Optional[float], Optional[float], bool]:
    """
    현재가/정가/밸류가격 추출 (USD 기준). 밸류가격 유무 반환.
    """
    txt = _clean_text(await card.text_content())
    # US$ 숫자 추출
    amounts = [float(m) for m in re.findall(r"US\$\s*([\d,]+(?:\.\d+)?)", txt)]
    # heuristics:
    # - 일반적으로 [현재가, 정가] 또는 [현재가, 정가, 밸류가] 순서로 나타남
    current = original = value = None
    has_value = False

    if amounts:
        current = amounts[0]
        if len(amounts) >= 2:
            original = amounts[1]
        if "Value" in txt or "VALUE" in txt:
            # 'Value US$xx'가 있다면 마지막 수치를 밸류로
            value = amounts[-1]
            has_value = True

    return current, original, value, has_value


async def _extract_brand(card: Locator, product_name: str) -> str:
    """
    카드 내 텍스트에서 가장 그럴듯한 브랜드 라인을 추출.
    실패 시 product_name의 선두 짧은 구간을 브랜드로 추정.
    """
    # 후보: 전형적인 클래스들
    for sel in [".brand", ".prd_brand", ".brandNm", ".sellers-brand", "strong"]:
        loc = card.locator(sel).first
        if await loc.count():
            t = _clean_text(await loc.text_content())
            if t and len(t) <= 40:
                return t

    # 라인 전체에서 골라내기 (짧고 가격/평점/할인 등이 아닌 첫 라인)
    lines = [l.strip() for l in (await card.inner_text()).splitlines()]
    for ln in lines:
        if not ln:
            continue
        if "US$" in ln or "%" in ln or "★" in ln or "Value" in ln:
            continue
        # 상품명보다 눈에 띄게 짧은 라인 우선
        if len(ln) <= 30:
            return ln

    # 마지막 fallback: product_name 선두의 1~3 단어 (대부분 커버됨)
    words = product_name.split()
    return " ".join(words[: min(3, len(words))])


async def _extract_one(card_anchor: Tuple[str, Locator], rank: int) -> Dict:
    pid, anchor = card_anchor
    card = await _closest_card(anchor)

    href = await anchor.get_attribute("href") or ""
    product_url = _absolutize(href)
    # 이미지
    img = card.locator("img").first
    image_url = ""
    if await img.count():
        src = await img.get_attribute("src")
        if src:
            image_url = _absolutize(src)

    # 상품명(타이틀 앵커의 텍스트)
    product_name = _clean_text(await anchor.text_content())
    # 브랜드
    brand = await _extract_brand(card, product_name)

    # 가격류
    price_current, price_original, value_price, has_value = await _extract_prices(card)
    discount_pct = _round_discount(price_current, price_original)

    return {
        "date_kst": _now_kst_date(),
        "rank": rank,
        "brand": brand,
        "product_name": product_name,
        "price_current_usd": price_current if price_current is not None else 0,
        "price_original_usd": price_original if price_original is not None else 0,
        "discount_rate_pct": discount_pct,  # 정수
        "value_price_usd": value_price if value_price is not None else 0,
        "has_value_price": has_value,
        "product_url": product_url,
        "image_url": image_url,
    }


# ---------- 외부에서 호출하는 메인 함수 ----------

async def scrape_oliveyoung_global() -> List[Dict]:
    """
    Top Orders(최대 100개) 수집.
    반환값: List[dict]
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)
        await _wait_until_idle(page, 600)

        container = await _get_top_orders_container(page)
        anchors = await _collect_unique_anchors(container, need=100)

        # rank 1..N 순서로 extract
        items: List[Dict] = []
        for idx, tup in enumerate(anchors, start=1):
            try:
                items.append(await _extract_one(tup, rank=idx))
            except Exception:
                # 한 항목 실패해도 전체 진행
                continue

        await ctx.close()
        await browser.close()
        return items
