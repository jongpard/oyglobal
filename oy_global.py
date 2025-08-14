import asyncio
import re
from typing import List, Dict

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
    for sel in [
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "button:has-text('Close')",
        ".cookie .close", ".popup .close", ".modal .btn-close"
    ]:
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
    # 'What's trending in Korea?' 헤더 Y좌표. 실패 시 매우 큰 값 반환.
    try:
        loc = page.get_by_text("What's trending in Korea?").first
        bb = await loc.bounding_box()
        if bb:
            return bb["y"]
    except:
        pass
    try:
        loc = page.get_by_text("What’s trending in Korea?").first  # 곡선 아포스트로피
        bb = await loc.bounding_box()
        if bb:
            return bb["y"]
    except:
        pass
    return 1e12

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        # 루트(있으면) 찾기
        root = None
        for sel in ["#pillsTab1Nav1", "[id*='pillsTab1']", "section [data-list='best-seller']"]:
            if await _wait_attached(page, sel, timeout=7000):
                root = sel
                break

        # 제품 링크가 DOM 붙을 때까지 대기
        await _wait_attached(page, "a[href*='product/detail']", timeout=15000)

        # 충분히 스크롤
        for _ in range(12):
            await page.mouse.wheel(0, 2600)
            await asyncio.sleep(0.9)

        trending_y = await _get_trending_boundary_y(page)
        scope_prefix = f"{root} " if root else ""
        links = await page.query_selector_all(f"{scope_prefix}a[href*='product/detail']")

        items: List[Dict] = []
        seen = set()
        rank = 0

        for a in links:
            try:
                bb = await a.bounding_box()
                if bb and bb["y"] >= trending_y:
                    # 트렌딩 섹션 이하 무시
                    continue

                href = await a.get_attribute("href")
                if not href:
                    continue
                product_url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                if product_url in seen:
                    continue
                seen.add(product_url)

                # 근접 카드
                card = await a.evaluate_handle("el => el.closest('li,div,article') || el")

                # 이미지
                img_el = await card.query_selector("img")
                img_url = (await img_el.get_attribute("src")) if img_el else None

                # 브랜드/상품명
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

                # 가격 텍스트(텍스트 + HTML 둘 다 활용)
                txt_card = await card.evaluate("el => (el.innerText || '').replace(/\\n/g, ' ')")
                html_card = await card.inner_html()
                price_block = (txt_card or "") + " " + (html_card or "")

                price_info = parse_prices_and_discount(price_block)
                if price_info.get("price_current_usd") is None:
                    continue  # 가격 없는 배너/기획전

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
