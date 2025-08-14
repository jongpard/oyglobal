import asyncio
import re
from typing import List, Dict

from playwright.async_api import async_playwright, BrowserContext
from price_parser import parse_prices_and_discount
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

async def _new_context(pw) -> BrowserContext:
    # 프록시/미국 IP 불필요. 기본 컨텍스트만 사용.
    browser = await pw.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-US"  # 텍스트 파싱 안정화를 위해 영문 선호(가격은 US$ 표기로 동일)
    )
    return context

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()
        await page.goto(BEST_URL, wait_until="networkidle", timeout=60000)

        # 목록 로딩 대기 (여러 셀렉터로 방어적으로 대기)
        selectors = [
            "section [data-list='best-seller']",
            "ul[class*='prd']", "ul[class*='product']",
            "div[class*='product-list']",
            "a[href*='product'] img"
        ]
        loaded = False
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=6000)
                loaded = True
                break
            except:
                pass
        if not loaded:
            await page.wait_for_selector("a[href*='product']", timeout=15000)

        # 지연 로딩 대비 스크롤 다운
        for _ in range(6):
            await page.mouse.wheel(0, 2200)
            await asyncio.sleep(0.8)

        # 카드 후보
        cards = await page.query_selector_all("li, div, article")
        items = []
        rank = 0

        for card in cards:
            try:
                a = await card.query_selector("a[href*='product']")
                if not a:
                    continue
                href = await a.get_attribute("href")
                if not href or "product" not in href:
                    continue
                product_url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"

                img = await card.query_selector("img")
                img_url = (await img.get_attribute("src")) if img else None

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

                price_block = await card.inner_text()
                price_info = parse_prices_and_discount(price_block)

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
        items = [it for it in items if it["rank"] > 0]
        items.sort(key=lambda x: x["rank"])
        return items
