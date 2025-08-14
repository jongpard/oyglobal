# -*- coding: utf-8 -*-
import asyncio
import re
from urllib.parse import urljoin
from datetime import datetime, timezone, timedelta

import pandas as pd
from playwright.async_api import async_playwright

KST = timezone(timedelta(hours=9))

HOME_URL = "https://global.oliveyoung.com/"

HEADERS = {
    "accept-language": "en-US,en;q=0.9,ko;q=0.8",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ---------- 유틸 ----------

def _clean_text(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def _to_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

USD_RE = re.compile(r"US\$\s*([0-9]+(?:\.[0-9]+)?)")
VALUE_RE = re.compile(r"(?i)value[^0-9]*US\$\s*([0-9]+(?:\.[0-9]+)?)")

def parse_prices_from_card_text(full_text: str):
    """
    카드 내부 텍스트를 기반으로
    - 현재가(current)
    - 정가(original)
    - Value 가격(value)
    를 추출한다.
    규칙:
      1) 텍스트에서 US$ 금액들을 모두 추출
      2) 'Value US$' 구문이 있으면 value_price로 처리
      3) 금액이 2개 이상이면 (작은값=current, 큰값=original)로 간주
         1개면 (current=그값, original=None)
    """
    text = full_text or ""
    # Value 먼저
    value_price = None
    m_val = VALUE_RE.search(text)
    if m_val:
        value_price = _to_float(m_val.group(1))

    # 모든 미국달러 가격
    prices = [ _to_float(m) for m in USD_RE.findall(text) ]
    current = None
    original = None
    if prices:
        if len(prices) == 1:
            current = prices[0]
        else:
            # 작은값 -> 현재가, 큰값 -> 정가 (보편적인 카드 패턴)
            current = min(prices)
            original = max(prices)

    # sanity: current == original 인 케이스
    if original is not None and current is not None and original <= current:
        # 정가가 현재가보다 작거나 같으면 정가 None 처리
        original = None

    return current, original, value_price

def calc_discount_pct(current, original):
    if current is None or original is None or original == 0:
        return None
    try:
        return round((1 - (current / original)) * 100, 2)
    except Exception:
        return None

def dedupe_brand_in_name(brand: str, name: str) -> str:
    """
    제품명에 브랜드가 중복으로 앞에 또 들어간 케이스 정리
    ex) "Beauty of Joseon Beauty of Joseon Relief Sun ..." -> 한 번만 남김
    """
    b = _clean_text(brand)
    n = _clean_text(name)
    if not b or not n:
        return n
    # 대소문자/공백 무시하여 중복 제거
    pat = re.compile(rf"^{re.escape(b)}\s+", flags=re.I)
    return re.sub(pat, "", n).strip()

# ---------- 스크래핑 ----------

SECTION_TITLE_RE = re.compile(r"(Best Sellers|Top Orders)", re.I)
TRENDING_TITLE_RE = re.compile(r"(What'?s trending in Korea)", re.I)

async def _get_section_locator(page, title_regex: re.Pattern):
    # 타이틀 텍스트 찾기
    heading = page.locator(f"text=/{title_regex.pattern}/{'' if title_regex.flags == 0 else 'i'}").first
    await heading().wait_for(timeout=30000)
    # 가장 가까운 section으로 올라가기
    section = heading().locator("xpath=ancestor-or-self::section[1]")
    await section.wait_for()
    return section

async def _autoscroll_until(page, locator, need=100, tries=30):
    """
    해당 locator의 개수가 need 이상이 될 때까지 천천히 스크롤
    """
    last_count = 0
    for _ in range(tries):
        count = await locator.count()
        if count >= need:
            return
        if count == last_count:
            # 약간 더 스크롤
            await page.mouse.wheel(0, 2000)
            await asyncio.sleep(0.6)
        else:
            last_count = count
            await asyncio.sleep(0.4)
    # 그래도 부족하면 그냥 진행

async def scrape_oliveyoung_global():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(extra_http_headers=HEADERS, locale="en-US")
        page = await ctx.new_page()
        await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)

        # 1) Best Sellers/Top Orders 섹션만 한정
        try:
            top_orders_section = await _get_section_locator(page, SECTION_TITLE_RE)
        except Exception:
            # 언어별 헤딩 탐색 실패 시, 화면 내 첫번째 product grid 섹션을 fallback
            grids = page.locator("section:has(a[href*='/product/detail'])").first
            await grids().wait_for(timeout=30000)
            top_orders_section = grids()

        # 섹션 내부의 상품 anchor들
        anchors = top_orders_section.locator("a[href*='/product/detail?']").first
        await _autoscroll_until(page, top_orders_section.locator("a[href*='/product/detail?']"), need=100)

        # 최종 anchor 목록
        cards = await top_orders_section.locator("a[href*='/product/detail?']").all()
        if not cards:
            # 섹션 내 anchor 하나도 없으면 홈 전체에서 fallback
            cards = await page.locator("a[href*='/product/detail?']").all()

        # 2) What’s trending 섹션 이후는 제외 (혹시 섞일 경우 방지)
        try:
            trending_section = await _get_section_locator(page, TRENDING_TITLE_RE)
            tbox = await trending_section.bounding_box()
            t_y = (tbox["y"] if tbox else 999999)
        except Exception:
            t_y = 999999  # 트렌딩 없으면 제한 없이

        items = []
        seen_prdt = set()
        rank = 0
        today = datetime.now(KST).strftime("%Y-%m-%d")

        for a in cards:
            # 트렌딩 영역 이후는 스킵
            try:
                abox = await a.bounding_box()
                if abox and abox["y"] >= t_y:
                    continue
            except Exception:
                pass

            href = await a.get_attribute("href")
            if not href:
                continue
            # 중복 제품 제거 (prdtNo 기준)
            m = re.search(r"prdtNo=([A-Za-z0-9]+)", href)
            prdt_no = m.group(1) if m else href
            if prdt_no in seen_prdt:
                continue
            seen_prdt.add(prdt_no)

            # 카드 루트(가장 가까운 li/div)로 올라가 텍스트/이미지 수집
            card_root = a.locator("xpath=ancestor-or-self::*[self::li or self::div][1]")

            # 이미지
            img = card_root.locator("img").first
            img_src = None
            try:
                img_src = await img.get_attribute("src")
            except Exception:
                pass
            image_url = img_src or ""

            # 브랜드/제품명
            brand = ""
            name = ""
            # 브랜드 후보: class에 brand 가 포함된 요소, 또는 카드 상단 텍스트
            for sel in [".brand, .txt_brand, .prd_brand", "a >> nth=0", "div >> nth=0"]:
                try:
                    t = _clean_text(await card_root.locator(sel).first.inner_text())
                    if t and 2 <= len(t) <= 60:
                        brand = t
                        break
                except Exception:
                    pass

            # 제품명 후보: 흔한 클래스들
            for sel in [".name, .prd_name, .title, .tit", "a >> nth=1", "a >> nth=0"]:
                try:
                    t = _clean_text(await card_root.locator(sel).first.inner_text())
                    if t and len(t) > 2:
                        name = t
                        break
                except Exception:
                    pass

            if not name:
                # anchor 자체 텍스트로 폴백
                try:
                    name = _clean_text(await a.inner_text())
                except Exception:
                    name = ""

            # 브랜드가 너무 비어있으면 이름에서 앞 단어를 브랜드로 폴백
            if not brand and name:
                head = name.split()[0]
                if head and len(head) <= 20:
                    brand = head

            # 이름에서 브랜드 중복 제거
            name = dedupe_brand_in_name(brand, name)

            # 카드 전체 텍스트로 가격들 파싱
            full_text = ""
            try:
                full_text = _clean_text(await card_root.inner_text())
            except Exception:
                pass

            price_current, price_original, value_price = parse_prices_from_card_text(full_text)
            discount_pct = calc_discount_pct(price_current, price_original)

            product_url = href if href.startswith("http") else urljoin(HOME_URL, href)

            rank += 1
            items.append({
                "date_kst": today,
                "rank": rank,
                "brand": brand,
                "product_name": name,
                "price_current_usd": price_current,
                "price_original_usd": price_original,
                "discount_rate_pct": discount_pct,
                "value_price_usd": value_price,
                "has_value_price": bool(value_price is not None),
                "product_url": product_url,
                "image_url": image_url,
            })

            if rank >= 100:
                break

        await ctx.close()
        await browser.close()

        # 필터: 가격/이름이 비정상(배너/카테고리)인 것 제거
        filtered = []
        for it in items:
            # 이름과 현재가가 있어야 정상 상품으로 간주
            if not it["product_name"] or it["price_current_usd"] is None:
                continue
            # "Makeup", "Suncare"같은 카테고리명 혼입 제거
            if it["product_name"].lower() in {"makeup", "suncare", "skincare", "hair", "face masks", "k-pop", "wellness", "supplements", "food & drink"}:
                continue
            filtered.append(it)

        # 순위 재정렬
        for idx, it in enumerate(filtered, start=1):
            it["rank"] = idx

        return filtered
