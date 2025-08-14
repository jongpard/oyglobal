# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com/"

# 아주 좁게: Top Orders(=베스트/인기) 섹션에서만 a[href*=product/detail] 추출
SEL_PRODUCT_ANCHOR = "a[href*='product/detail']"

# KST
KST = timezone(timedelta(hours=9))

def _now_kst_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def _to_float(s: str) -> float:
    try:
        return float(s)
    except Exception:
        return 0.0

def _split_brand_and_product(anchor_text: str) -> (str, str):
    """
    카드 앵커 텍스트는 보통
      1행: 브랜드
      2행: 제품명(브랜드 포함일 때도 있어서 정규식으로 앞 브랜드 제거)
    형태라 줄단위로 나눈 뒤, 첫 줄=브랜드, 나머지 합친 뒤
    맨 앞 브랜드명 반복되면 제거합니다.
    """
    lines = [ln.strip() for ln in anchor_text.splitlines() if ln.strip()]
    if not lines:
        return "", ""

    brand = lines[0]
    rest = " ".join(lines[1:]).strip()

    if not rest:
        # 라인이 하나뿐이면, 브랜드가 앵커 전체를 차지하는 케이스.
        # 이런 경우는 보통 그 아래 다른 노드에 제품명이 분리돼 있지 않아서
        # 앵커 전체 텍스트에서 브랜드 반복 제거를 시도
        txt = " ".join(lines)
        # "BRAND BRAND something" 같은 패턴 정리
        if txt.lower().startswith((brand + " ").lower()):
            rest = txt[len(brand):].strip()
        else:
            rest = txt

    # 제품명에 브랜드가 한 번 더 앞에 붙어 있으면 제거
    pattern = re.compile(rf"^{re.escape(brand)}\s+", re.IGNORECASE)
    product = pattern.sub("", rest).strip()

    return brand, product

_price_cur_re = re.compile(r"US\$\s*([0-9]+(?:\.[0-9]+)?)")
_price_val_re = re.compile(r"(?:정가|Value)\s*[: ]?\s*US\$\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

def _parse_prices(text: str) -> Dict[str, float | int]:
    """
    카드 전체 텍스트에서 현재가/정가(=value) 추출.
    - 현재가: 텍스트 상 첫 번째 US$ 숫자
    - 정가(value): '정가' 또는 'Value' 수식어가 붙은 US$ 숫자 (없으면 0)
    할인율은 둘 다 있을 때 계산
    """
    cur = 0.0
    val = 0.0

    m_cur = _price_cur_re.search(text)
    if m_cur:
        cur = _to_float(m_cur.group(1))

    m_val = _price_val_re.search(text)
    if m_val:
        val = _to_float(m_val.group(1))

    disc = 0.0
    if val > 0 and cur > 0 and val >= cur:
        disc = round((val - cur) / val * 100, 2)

    return dict(
        price_current_usd=cur,
        price_original_usd=val,   # '정가/Value'를 원가로 사용
        discount_rate_pct=disc,
        value_price_usd=val,
        has_value_price=1 if val > 0 else 0,
    )

async def _get_section_locator(page: Page, title_regex: re.Pattern) -> Locator:
    """
    페이지에서 'Top Orders'/'Best Sellers' 류 타이틀(h2/h3)을 찾아
    가장 가까운 섹션 컨테이너(ancestor section/div)를 반환
    """
    heading = page.locator("h2, h3").filter(has_text=title_regex).first
    await heading.wait_for(state="visible", timeout=30000)
    # 섹션 또는 가장 가까운 div로 한정
    section = heading.locator("xpath=ancestor::*[self::section or self::div][1]")
    return section

async def _autoscroll_collect(section: Locator, need: int = 100) -> List[Locator]:
    """
    섹션 안에서 상품 카드 a[href*=product/detail]를 모으면서 자동 스크롤.
    need개 이상 또는 더 이상 늘어나지 않으면 종료.
    """
    seen = set()
    anchors: List[Locator] = []

    # 섹션 맨 위부터
    await section.scroll_into_view_if_needed()

    idle_rounds = 0
    while True:
        await asyncio.sleep(0.3)
        new_found = 0

        # 현재 보이는 앵커들
        batch = section.locator(SEL_PRODUCT_ANCHOR)
        count = await batch.count()
        for i in range(count):
            a = batch.nth(i)
            href = await a.get_attribute("href") or ""
            if not href:
                continue
            if href in seen:
                continue

            # 카드 루트(가장 가까운 상품 카드) 잡고, 화면에 스크롤
            card = a.locator("xpath=ancestor::*[self::li or self::div][1]")
            try:
                await card.scroll_into_view_if_needed()
            except Exception:
                pass

            anchors.append(a)
            seen.add(href)
            new_found += 1

        if len(anchors) >= need:
            break

        if new_found == 0:
            idle_rounds += 1
        else:
            idle_rounds = 0

        # 더 이상 늘지 않으면 탈출
        if idle_rounds >= 5:
            break

        # 아래로 조금 더 스크롤 유도
        try:
            await section.evaluate("(el) => el.scrollBy(0, el.clientHeight)")
        except Exception:
            break

    return anchors[:need]

async def _extract_item_from_anchor(a: Locator) -> Dict:
    """
    단일 앵커에서 상품 정보 추출 (브랜드/제품명/가격/URL/이미지)
    """
    href = await a.get_attribute("href") or ""
    product_url = href if href.startswith("http") else (BASE_URL.rstrip("/") + "/" + href.lstrip("/"))

    # 카드 전체 텍스트(가격 포함)를 얻기 위해 카드 루트 텍스트 사용
    card = a.locator("xpath=ancestor::*[self::li or self::div][1]")
    card_text = await card.inner_text()

    # 앵커 텍스트로 브랜드/상품명 분리
    anchor_text = await a.inner_text()
    brand, product = _split_brand_and_product(anchor_text)

    # 이미지
    img = ""
    try:
        img = await a.locator("img").first.get_attribute("src") or ""
    except Exception:
        pass

    prices = _parse_prices(card_text)

    return dict(
        brand=brand,
        product_name=product or brand,  # 혹시 못 뽑아도 비어있지 않게
        price_current_usd=prices["price_current_usd"],
        price_original_usd=prices["price_original_usd"],
        discount_rate_pct=prices["discount_rate_pct"],
        value_price_usd=prices["value_price_usd"],
        has_value_price=bool(prices["has_value_price"]),
        product_url=product_url,
        image_url=img,
    )

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1366, "height": 900})
        await page.goto(BASE_URL, wait_until="domcontentloaded")

        # Top Orders / Best Sellers 등으로 보이는 섹션을 폭넓게 매칭
        title_re = re.compile(r"(Top\s*Orders|Best\s*Sellers|TOP\s*100|TOP\s*50|TOP\s*10)", re.I)
        section = await _get_section_locator(page, title_re)

        # 섹션 안에서 제품 앵커 대기 → 수집
        await section.locator(SEL_PRODUCT_ANCHOR).first.wait_for(state="visible", timeout=30000)
        anchors = await _autoscroll_collect(section, need=100)

        items: List[Dict] = []
        for a in anchors:
            try:
                data = await _extract_item_from_anchor(a)
                items.append(data)
            except Exception:
                continue

        await browser.close()

    # 랭크/날짜 부여
    date_kst = _now_kst_date()
    for i, it in enumerate(items, start=1):
        it["date_kst"] = date_kst
        it["rank"] = i

    return items


# ----------------- CSV 저장 유틸 -----------------
import csv
from pathlib import Path

CSV_HEADER = [
    "date_kst",
    "rank",
    "brand",
    "product_name",
    "price_current_usd",
    "price_original_usd",
    "discount_rate_pct",
    "value_price_usd",
    "has_value_price",
    "product_url",
    "image_url",
]

def save_csv(items: List[Dict], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writeheader()
        for it in items:
            row = {k: it.get(k, "") for k in CSV_HEADER}
            w.writerow(row)


# ----------------- 엔트리 포인트 -----------------
if __name__ == "__main__":
    import os
    out = f"data/oliveyoung_global_{_now_kst_date()}.csv"
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = asyncio.run(scrape_oliveyoung_global())  # List[dict]
    save_csv(items, out)
    print(f"📁 저장 완료: {out}")
    # 슬랙은 별도 step에서 처리
