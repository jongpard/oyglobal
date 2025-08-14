# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com/"
SEL_PRODUCT_ANCHOR = "a[href*='product/detail']"

KST = timezone(timedelta(hours=9))


# ---------- helpers ----------

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _parse_usd(s: str) -> float:
    """
    US$31.86, US$ 44.00, 31.86 등에서 숫자만 추출
    """
    if not s:
        return 0.0
    m = re.search(r"([\d]{1,3}(?:[,\d]{0,3})*(?:\.\d{1,2})?)", s.replace(",", ""))
    return float(m.group(1)) if m else 0.0


def _round_pct(current: float, original: float) -> int:
    if not original or original <= 0:
        return 0
    pct = (1.0 - (current / original)) * 100.0
    # 반올림, 음수 보호
    try:
        return int(round(pct))
    except Exception:
        return 0


async def _first_text(node: Locator, selectors: List[str]) -> str:
    for sel in selectors:
        loc = node.locator(sel).first
        try:
            t = _clean_text(await loc.text_content())
            if t:
                return t
        except Exception:
            pass
    return ""


async def _first_attr(node: Locator, selectors: List[str], attr: str) -> str:
    for sel in selectors:
        loc = node.locator(sel).first
        try:
            v = await loc.get_attribute(attr)
            if v:
                return v
        except Exception:
            pass
    return ""


# ---------- robust section locator ----------

async def locate_top_orders_section(page: Page) -> Locator:
    """
    1) Top Orders/Best Sellers/TOP.. 헤딩 시도
    2) 실패 시 '상품 앵커 밀도'가 높은 섹션 자동 선택
       + “What’s trending in Korea” 이하 섹션은 제외
    """
    page.set_default_timeout(45000)
    await page.wait_for_load_state("domcontentloaded")

    # 탭이 있으면 클릭 시도 (있을 때만)
    try:
        await page.get_by_role("tab", name=re.compile(r"(Top\s*Orders|Best\s*Sellers)", re.I)).click(timeout=3000)
    except Exception:
        pass

    # 1) 헤딩 기반
    heading_pats = [
        re.compile(r"(Top\s*Orders|Best\s*Sellers|TOP\s*100|TOP\s*50|TOP\s*10)", re.I),
        re.compile(r"(베스트|인기|주문\s*상위)", re.I),
    ]
    for pat in heading_pats:
        cand = page.locator("h1,h2,h3,h4").filter(has_text=pat).first
        try:
            await cand.wait_for(state="visible", timeout=5000)
            return cand.locator("xpath=ancestor::*[self::section or self::div][1]")
        except Exception:
            pass

    # 2) 폴백: 앵커 밀도 높은 섹션 자동 선택(트렌딩 이하 제외)
    trending = page.locator(":text(\"What's trending in Korea\")").first
    trending_y = 10 ** 9
    try:
        bb = await trending.bounding_box()
        if bb:
            trending_y = bb["y"]
    except Exception:
        pass

    candidates = page.locator("section, div")
    best = None
    best_count = 0
    count = await candidates.count()
    # 너무 많으면 상단부터 250개만 검사
    for i in range(min(count, 250)):
        sec = candidates.nth(i)
        try:
            bb = await sec.bounding_box()
            if bb and bb["y"] >= trending_y:
                continue
            cnt = await sec.locator(SEL_PRODUCT_ANCHOR).count()
            if cnt > best_count:
                best = sec
                best_count = cnt
        except Exception:
            continue

    if best:
        return best
    # 최후 폴백
    return page.locator("body")


async def _autoscroll_collect(scope: Locator, need: int = 100) -> List[str]:
    """
    scope 내부의 상품 anchor href를 스크롤하면서 최대 need개까지 수집 (중복 제거)
    """
    hrefs: List[str] = []
    seen = set()
    last_len = -1
    stall = 0

    for _ in range(60):  # 안전 상한
        # 새 href 수집
        try:
            batch = await scope.eval_on_selector_all(
                SEL_PRODUCT_ANCHOR, "els => els.map(e => e.href)"
            )
        except Exception:
            batch = []

        for h in batch:
            if h and h not in seen:
                seen.add(h)
                hrefs.append(h)
                if len(hrefs) >= need:
                    return hrefs[:need]

        # 더 안 늘어나면 스톨 증가
        if len(hrefs) == last_len:
            stall += 1
        else:
            stall = 0
        last_len = len(hrefs)

        # 스크롤
        try:
            await scope.evaluate("el => el.scrollBy(0, 1200)")
        except Exception:
            try:
                await scope.evaluate("el => el.scrollIntoView({behavior:'instant'})")
            except Exception:
                pass
        await asyncio.sleep(0.3)

        if stall >= 6:  # 6번 연속 정체면 종료
            break

    return hrefs[:need]


async def _parse_card_from_href(page: Page, scope: Locator, href: str) -> Dict:
    """
    href를 기준으로 카드 컨테이너를 찾아 정보 추출
    """
    a = scope.locator(f"a[href='{href}']").first
    # 카드 컨테이너 (li/div)
    card = a.locator("xpath=ancestor::*[self::li or self::div][1]")

    # 브랜드 & 제품명
    brand = await _first_text(card, [
        ".tx_brand", ".brand", ".product-brand", ".prd_brand", "em.tx_brand", "em.brand", "em",
    ])

    name = await _first_text(card, [
        ".tx_name", ".name", ".product_name", ".prd_name", "p.name", "p.tx_name", "a[title]"
    ])
    if not name:
        alt = await _first_attr(card, ["img", "img.product", "img.prd_img"], "alt")
        name = alt or name

    brand = _clean_text(brand)
    name = _clean_text(name)

    # 가격(현재/정가/밸류)
    text_all = ""
    try:
        text_all = await card.inner_text()
    except Exception:
        pass

    # value price
    value_price_usd = 0.0
    m_val = re.search(r"\(Value[:\s]*US\$\s*([\d.,]+)\)", text_all, re.I)
    if m_val:
        value_price_usd = _parse_usd(m_val.group(1))

    # 모든 'US$xx' 숫자 추출 → 일반적으로 큰 값이 정가, 작은 값이 현재가
    all_prices = [float(x) for x in re.findall(r"US\$\s*([\d]+(?:\.\d{1,2})?)", text_all)]
    price_current, price_original = 0.0, 0.0
    if all_prices:
        price_current = min(all_prices)
        price_original = max(all_prices)
    # 혹시 둘 다 0이면 a 주변 텍스트 추가 시도
    if price_current == 0 or price_original == 0:
        around = _clean_text(await a.text_content() or "")
        extra = [float(x) for x in re.findall(r"US\$\s*([\d]+(?:\.\d{1,2})?)", around)]
        if extra:
            price_current = price_current or min(extra)
            price_original = price_original or max(extra)

    discount_rate_pct = _round_pct(price_current, price_original)
    has_value_price = bool(value_price_usd and value_price_usd > 0)

    # 이미지
    image_url = await _first_attr(card, ["img", "img.product", "img.prd_img"], "src")
    if not image_url:
        image_url = await _first_attr(card, ["img", "img.product", "img.prd_img"], "data-src")

    return {
        "brand": brand,
        "product_name": name,
        "price_current_usd": price_current,
        "price_original_usd": price_original,
        "discount_rate_pct": discount_rate_pct,  # 정수
        "value_price_usd": value_price_usd,
        "has_value_price": has_value_price,
        "product_url": href,
        "image_url": image_url or "",
    }


# ---------- public: main scraping ----------

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded")

        section = await locate_top_orders_section(page)

        # 섹션이 보이도록 위치 조정
        try:
            await section.scroll_into_view_if_needed(timeout=5000)
        except Exception:
            pass

        hrefs = await _autoscroll_collect(section, need=100)
        # 안전장치: 중복 제거
        hrefs = list(dict.fromkeys(hrefs))[:100]

        items: List[Dict] = []
        rank = 1
        for href in hrefs:
            try:
                data = await _parse_card_from_href(page, section, href)
                data["date_kst"] = datetime.now(KST).strftime("%Y-%m-%d")
                data["rank"] = rank
                items.append(data)
                rank += 1
            except Exception:
                continue

        await context.close()
        await browser.close()

        return items
