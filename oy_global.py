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
    # 쿠키 배너/팝업 닫기(있으면)
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
                await page.click(sel, timeout=1000)
                await asyncio.sleep(0.3)
        except:
            pass

async def _soft_wait(page, selector, timeout=15000):
    # visible 대신 attached로 완화(가시성 이슈 방지)
    try:
        await page.wait_for_selector(selector, state="attached", timeout=timeout)
        return True
    except:
        return False

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        # 진입 및 초기 로드 대기 강화
        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        # 베스트셀러 탭 루트가 있으면 그 안에서 대기
        root_sel_candidates = [
            "#pillsTab1Nav1",
            "[id*='pillsTab1']",
            "section [data-list='best-seller']"
        ]
        root = None
        for sel in root_sel_candidates:
            ok = await _soft_wait(page, sel, timeout=8000)
            if ok:
                root = sel
                break

        # 어쨌든 제품 a태그가 DOM에 붙을 때까지 느슨히 대기 (가시성 X)
        await _soft_wait(page, "a[href*='product/detail']", timeout=15000)

        # 지연 로딩 대비 스크롤 충분히
        # (상황에 따라 10~14회까지 증가)
        for _ in range(12):
            await page.mouse.wheel(0, 2600)
            await asyncio.sleep(0.9)

        # 검색 범위: 가능한 경우 루트 내부를 우선, 아니면 문서 전체
        scope_prefix = f"{root} " if root else ""
        # 카드 후보 넓게 수집(배너/기획전도 섞여 있으므로 아래에서 가격 필터링)
        cards = await page.query_selector_all(f"{scope_prefix}li, {scope_prefix}div, {scope_prefix}article")

        items: List[Dict] = []
        rank = 0

        for card in cards:
            try:
                # 제품 상세 링크
                a = await card.query_selector("a[href*='product/detail']")
                if not a:
                    continue
                href = await a.get_attribute("href")
                if not href or "product/detail" not in href:
                    continue
                product_url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"

                # 이미지
                img = await card.query_selector("img")
                img_url = (await img.get_attribute("src")) if img else None

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

                # 가격 블록 텍스트
                price_block = await card.inner_text()
                price_info = parse_prices_and_discount(price_block)

                # 가격 정보 없는(배너 등) 카드는 스킵
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
        # 정렬 및 정리
        items = [it for it in items if it["rank"] > 0]
        items.sort(key=lambda x: x["rank"])
        return items
