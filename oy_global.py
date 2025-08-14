import asyncio
import re
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, BrowserContext
from price_parser import parse_prices_and_discount
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

async def _new_context(pw) -> BrowserContext:
    browser = await pw.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-US"
    )
    return context

async def _try_close_overlays(page):
    # 쿠키/팝업 닫기 (있을 때만)
    selectors = [
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "button:has-text('Close')",
        ".cookie .close", ".popup .close", ".modal .btn-close"
    ]
    for sel in selectors:
        try:
            if await page.is_visible(sel):
                await page.click(sel, timeout=500)
                await asyncio.sleep(0.2)
        except:
            pass

async def _wait_attached(page, selector, timeout=15000) -> bool:
    try:
        await page.wait_for_selector(selector, state="attached", timeout=timeout)
        return True
    except:
        return False

async def _get_trending_boundary_y(page) -> float:
    """
    'What's trending in Korea?' 섹션의 y 좌표를 얻어,
    그 아래 카드들은 무시한다. 없으면 매우 큰 값 반환.
    """
    candidates = [
        "text=What's trending in Korea?",
        "text=What’s trending in Korea?"
    ]
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                bb = await el.bounding_box()
                if bb:
                    return bb["y"]
        except:
            continue
    return 1e12  # sentinel

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        # 진입
        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        # 베스트 섹션 루트 대기(느슨하게)
        root_sel_candidates = [
            "#pillsTab1Nav1",
            "[id*='pillsTab1']",
            "section [data-list='best-seller']"
        ]
        root = None
        for sel in root_sel_candidates:
            if await _wait_attached(page, sel, timeout=7000):
                root = sel
                break

        # 제품 상세 링크가 DOM에 붙을 때까지(가시성 X)
        await _wait_attached(page, "a[href*='product/detail']", timeout=15000)

        # 지연 로딩 대비 충분히 스크롤
        for _ in range(12):
            await page.mouse.wheel(0, 2600)
            await asyncio.sleep(0.9)

        trending_y = await _get_trending_boundary_y(page)

        # 링크 자체를 기준으로 수집 (타일 대부분이 a로 래핑됨)
        scope_prefix = f"{root} " if root else ""
        links = await page.query_selector_all(f"{scope_prefix}a[href*='product/detail']")

        items: List[Dict] = []
        seen = set()  # 중복 URL 제거
        rank = 0

        for a in links:
            try:
                # 트렌딩 섹션 아래는 무시
                bb = await a.bounding_box()
                if bb and bb["y"] >= trending_y:
                    continue

                href = await a.get_attribute("href")
                if not href:
                    continue
                product_url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                if product_url in seen:
                    continue
                seen.add(product_url)

                # 카드 컨테이너 (브랜드/이름/이미지/가격)
                card = await a.evaluate_handle("el => el.closest('li,div,article') || el")

                # 이미지
                img_el = await card.query_selector("img")
                img_url = (await img_el.get_attribute("src")) if img_el else None

                # 브랜드/이름
                brand = None
                for sel in ["strong.brand", ".brand", ".prd_brand", ".product-brand"]:
                    el = await card.query_selector(sel)
                    if el:
                        t = (await el.inner_text()).strip()
                        if t:
                            brand = t
                            break

                product_name = None
                for sel in ["p.name", ".name", ".prd_name", ".product-name", "strong.name"]:
                    el = await card.query_selector(sel)
                    if el:
                        t = (await el.inner_text()).strip()
                        if t:
                            product_name = re.sub(r"\s+", " ", t)
                            break
                if not product_name:
                    t = (await a.inner_text()).strip()
                    product_name = re.sub(r"\s+", " ", t)

                # 가격 텍스트: a 내부 + 부모컨테이너 텍스트 모두 포함
                txt_a = (await a.inner_text()).replace("\n", " ")
                txt_card = await card.evaluate("el => (el.innerText || '').replace(/\\n/g, ' ')")
                price_block = (txt_card or "") + " " + (txt_a or "")

                price_info = parse_prices_and_discount(price_block)

                # 가격 없으면 배너/기획전 → 스킵
                if price_info.get("price_current_usd") is None:
                    continue

                rank += 1
                items.append({
                    "date_kst": kst_today_str(),
                    "rank": rank,
                    "brand": brand,
                    "product_name": product_name,
                    "price_current_usd": price_info["price_current_usd"],
                    "price_original_usd": price_info["price_original_usd"],
                    "discount_rate_pct": price_info["discount_rate_pct"],
                    "value_price_usd": price_info["value_price_usd"],
                    "has_value_price": price_info["has_value_price"],
                    "product_url": product_url,
                    "image_url": img_url
                })
            except Exception:
                continue

        await context.close()

        items.sort(key=lambda x: x["rank"])
        return items
