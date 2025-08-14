import asyncio
import re
from typing import List, Dict, Tuple

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

async def _wait_attached(page, selector, timeout=20000) -> bool:
    try:
        await page.wait_for_selector(selector, state="attached", timeout=timeout)
        return True
    except:
        return False

async def _gather_price_blob(card_handle) -> str:
    """카드의 텍스트/HTML/자식 속성까지 긁어서 가격 파싱에 쓰는 블롭을 만든다."""
    try:
        return await card_handle.evaluate(
            """(el) => {
                const parts = [];
                const txt = (el.innerText || '').replace(/\\n/g, ' ');
                const html = (el.innerHTML || '').replace(/\\n/g, ' ');
                parts.push(txt, html);
                const walker = document.createTreeWalker(el, NodeFilter.SHOW_ELEMENT, null);
                while (walker.nextNode()) {
                  const n = walker.currentNode;
                  for (const attr of Array.from(n.attributes || [])) parts.push(attr.value);
                  const aria = n.getAttribute && n.getAttribute('aria-label');
                  if (aria) parts.push(aria);
                  const before = window.getComputedStyle(n, '::before').content;
                  const after  = window.getComputedStyle(n, '::after').content;
                  if (before && before !== 'none') parts.push(before);
                  if (after  && after  !== 'none') parts.push(after);
                }
                return parts.join(' ');
            }"""
        )
    except:
        try:
            return await card_handle.evaluate("(el)=> (el.innerText||'') + ' ' + (el.innerHTML||'')")
        except:
            return ""

async def _find_trending_bounds(page) -> Tuple[float, float]:
    """
    'What's trending in Korea?' 섹션의 y-범위를 추정해 상단(1~10)과 하단(11~100)을 모두 포함하고
    트렌딩만 제외한다. 못 찾으면 (inf, -inf) 반환해서 필터를 비활성화.
    """
    tops = []
    for txt in ["What's trending in Korea?", "What’s trending in Korea?"]:
        try:
            loc = page.get_by_text(txt).first
            bb = await loc.bounding_box()
            if bb: tops.append(bb["y"])
        except:
            pass
    if not tops:
        return float("inf"), float("-inf")  # not found → 필터 비활성화

    top_y = min(tops)

    # 트렌딩 그리드의 대충 높이(여유있게)
    # 실제 스크린샷 기준 400~700px 정도 → 넉넉히 1200px
    bottom_y = top_y + 1200.0
    return top_y, bottom_y

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        await _wait_attached(page, "a[href*='product/detail']", timeout=20000)

        # 전체 로드(최대 100위까지) 위해 충분히 스크롤
        for _ in range(18):
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(0.8)

        # 모든 제품 링크 수집
        all_links = await page.query_selector_all("a[href*='product/detail']")

        # 링크를 y좌표와 함께 모음
        triplets: List[Tuple[float, str, object]] = []
        for a in all_links:
            try:
                bb = await a.bounding_box()
                if not bb:
                    continue
                href = await a.get_attribute("href")
                if not href:
                    continue
                url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                triplets.append((bb["y"], url, a))
            except:
                continue

        triplets.sort(key=lambda t: t[0])
        trending_top, trending_bottom = await _find_trending_bounds(page)

        # 트렌딩 범위를 제외하고(위 + 아래) 선택
        selected: List[Tuple[str, object]] = []
        seen = set()
        for y, url, a in triplets:
            if trending_top <= y <= trending_bottom:
                continue  # 트렌딩 영역 컷
            if url in seen:
                continue
            seen.add(url)
            selected.append((url, a))

        # 상단(트렌딩 위쪽) 먼저, 그 다음(트렌딩 아래쪽) 순서 유지
        selected.sort(key=lambda t: next(y for y, u, a in triplets if u == t[0]))

        print(f"🔎 링크 전체: {len(triplets)}개, 선택(트렌딩 제외): {len(selected)}개, "
              f"트렌딩 y=({trending_top:.1f}~{trending_bottom:.1f})")

        items: List[Dict] = []
        rank = 0
        parsed_ok = 0

        for url, a in selected:
            try:
                card = await a.evaluate_handle("el => el.closest('li,div,article') || el")

                img_el = await card.query_selector("img")
                img_url = (await img_el.get_attribute("src")) if img_el else None

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
