# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from playwright.async_api import async_playwright, Page, Locator


BASE_URL = "https://global.oliveyoung.com/"
# 배너(히어로 드로우) 때문에 첫 anchor 대기가 꼬이지 않도록, 배너를 셀렉터에서 제외
SEL_PRODUCT_ANCHOR = "a[href*='product/detail']:not([data-banneridx])"


# ---------- utilities ----------

KST = timezone(timedelta(hours=9))


def _today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


async def _sleep(ms: int) -> None:
    await asyncio.sleep(ms / 1000)


async def _scroll_until(page: Page, locator: Locator, need: int, max_loops: int = 80) -> None:
    """
    lazy-load 리스트가 충분히 붙을 때까지 아래로 스크롤
    """
    last_count = -1
    for i in range(max_loops):
        count = await locator.count()
        if count >= need:
            return
        # 더 이상 늘지 않으면 그래도 2~3번 더 스크롤
        if count == last_count and i > 5:
            await page.evaluate("window.scrollBy(0, window.innerHeight * 0.9)")
        else:
            await page.evaluate("window.scrollBy(0, window.innerHeight * 1.5)")
        last_count = count
        await _sleep(250)
    # need 미만이어도 여기서 종료 → 이후 필터링으로 실제 개수 확정


# ---------- core scraping ----------

async def _get_trending_header_top(page: Page) -> float:
    """
    'What's trending in Korea' 헤더의 y좌표. 없으면 매우 큰 값 반환(즉, 필터링 무시).
    """
    try:
        trend = page.locator(r"text=/what'?s\s+trending\s+in\s+korea/i").first
        await trend.wait_for(state="attached", timeout=1000)
        box = await trend.bounding_box()
        if box and "y" in box:
            return float(box["y"])
    except Exception:
        pass
    return 10 ** 9  # 존재하지 않으면 사실상 무한대로


async def _collect_cards(page: Page) -> List[Dict]:
    """
    Top Orders(베스트) 영역의 카드만 수집.
    - 배너(anchor data-banneridx)는 처음부터 셀렉터에서 제외
    - 트렌딩 섹션(What's trending in Korea) 아래의 카드는 버림
    """
    all_links = page.locator(SEL_PRODUCT_ANCHOR)

    # 첫 anchor가 붙기만 하면 진행 (visible 대기 아님)
    await all_links.first.wait_for(state="attached", timeout=30000)

    # 레이지로드 한 번 깨우기
    await page.evaluate("window.scrollBy(0, 300)")
    await _sleep(400)

    # 충분히 붙을 때까지 스크롤
    await _scroll_until(page, all_links, need=140, max_loops=80)

    trending_top = await _get_trending_header_top(page)

    # 각 anchor에서 카드 정보를 뽑는다 (브라우저 컨텍스트에서)
    js = r"""
    (e) => {
      const card = e.closest('li, .prd_item, .product-item, .prod-item, .product, .item, .goods_item') || e.parentElement;
      const pick = (sel) => card && card.querySelector(sel);
      const text = (sel) => {
        const el = pick(sel);
        return el ? el.textContent.trim() : '';
      };

      // 브랜드/상품명 후보
      const brand = text('[class*=brand], .brand, .prd_brand, .product-brand');
      let name = text('[class*=name], .name, .tit, .title, .product-name, .prd_name');
      if (!name) {
        name = e.getAttribute('title') || e.textContent.trim();
      }

      // 가격 추출
      const wholeText = (card ? card.textContent : e.textContent) || '';
      const money = [...wholeText.matchAll(/US\$([\d,.]+)/g)].map(m => m[1].replace(/,/g,''));
      let priceCur = null, priceOrig = null;
      if (money.length >= 1) {
        priceCur = parseFloat(money[0]);
      }
      // 정가 또는 Value 가격
      const mOrig = wholeText.match(/(?:정가|Value)\s*US\$([\d,.]+)/i);
      if (mOrig) {
        priceOrig = parseFloat(mOrig[1].replace(/,/g,''));
      } else if (money.length >= 2) {
        priceOrig = parseFloat(money[1]);
      }
      const mDisc = wholeText.match(/↓\s*([\d.,]+)%/);
      const discountPct = mDisc ? parseFloat(mDisc[1].replace(/,/g,'')) : null;

      const imgEl = (card && card.querySelector('img')) || null;
      const img = imgEl ? (imgEl.currentSrc || imgEl.src) : null;

      const rect = e.getBoundingClientRect();
      const y = rect.top + (window.scrollY || document.documentElement.scrollTop || 0);

      const hasValue = /Value\s*US\$/i.test(wholeText);

      return {
        brand: (brand || '').trim(),
        product_name: (name || '').trim(),
        price_current_usd: priceCur,
        price_original_usd: priceOrig,
        discount_rate_pct: discountPct,
        has_value_price: hasValue,
        product_url: e.href,
        image_url: img,
        y
      };
    }
    """

    handles = await all_links.element_handles()
    rows: List[Dict] = []
    for h in handles:
        data = await h.evaluate(js)
        # 트렌딩 섹션 아래면 버림
        if data.get("y", 0) >= trending_top:
            continue
        # 배너/광고/잘못된 카드(가격/상품명 누락) 제거
        name_ok = bool(data.get("product_name"))
        url_ok = bool(data.get("product_url"))
        # 가격은 0일 수도 있으니 None만 거른다
        cur_ok = (data.get("price_current_usd") is not None)
        if not (name_ok and url_ok and cur_ok):
            continue

        # 브랜드에 상품명이 섞이는 것을 방지: 브랜드가 상품명과 동일하면 비움
        brand = (data.get("brand") or "").strip()
        pname = (data.get("product_name") or "").strip()
        if brand and pname and brand == pname:
            brand = ""
        data["brand"] = brand

        rows.append({
            "brand": brand,
            "product_name": pname,
            "price_current_usd": data.get("price_current_usd"),
            "price_original_usd": data.get("price_original_usd"),
            "discount_rate_pct": data.get("discount_rate_pct"),
            "has_value_price": bool(data.get("has_value_price")),
            "product_url": data.get("product_url"),
            "image_url": data.get("image_url"),
        })

    # 순서를 그대로 랭크로 사용
    date_kst = _today_kst()
    result: List[Dict] = []
    for i, r in enumerate(rows[:100], start=1):
        r2 = {
            "date_kst": date_kst,
            "rank": i,
            **r,
        }
        result.append(r2)
    return result


# ---------- public API ----------

async def scrape_oliveyoung_global() -> List[Dict]:
    """
    올리브영 글로벌 Top Orders 1~100위 수집.
    배너/트렌딩/라벨 카드(가격 없는 카드)는 제외.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            viewport={"width": 1366, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

        # 메인 진입 후 잠깐 대기 (상단 배너/레이어 로드)
        await _sleep(600)

        items = await _collect_cards(page)

        await context.close()
        await browser.close()
        return items
