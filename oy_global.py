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

async def _wait_attached(page, selector, timeout=15000) -> bool:
    try:
        await page.wait_for_selector(selector, state="attached", timeout=timeout)
        return True
    except:
        return False

async def _gather_price_blob(card_handle) -> str:
    """카드 요소의 텍스트/HTML/모든 자식 속성값을 긁어서 하나의 문자열로 반환."""
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
                  for (const attr of Array.from(n.attributes || [])) {
                    parts.push(attr.value);
                  }
                  const aria = n.getAttribute && n.getAttribute('aria-label');
                  if (aria) parts.push(aria);
                  const content = window.getComputedStyle(n, '::before').content;
                  if (content && content !== 'none') parts.push(content);
                }
                return parts.join(' ');
            }"""
        )
    except:
        try:
            return await card_handle.evaluate("(el)=> (el.innerText||'') + ' ' + (el.innerHTML||'')")
        except:
            return ""

def _first_big_gap_cutoff(sorted_y: List[float], min_gap: float = 140.0) -> float:
    """
    y좌표를 정렬한 뒤, 인접한 항목 간 '큰 갭'을 찾아
    그 다음 y를 컷오프로 반환. 없으면 아주 큰 값 반환.
    """
    if len(sorted_y) < 2:
        return 1e12
    for i in range(len(sorted_y) - 1):
        if sorted_y[i+1] - sorted_y[i] >= min_gap:
            return sorted_y[i+1]
    return 1e12

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        # 제품 링크 DOM 부착 대기
        await _wait_attached(page, "a[href*='product/detail']", timeout=20000)

        # 지연 로딩 대비 충분히 스크롤
        for _ in range(14):
            await page.mouse.wheel(0, 2800)
            await asyncio.sleep(0.9)

        # 페이지 내 모든 제품 링크 수집
        all_links = await page.query_selector_all("a[href*='product/detail']")
        link_triplets: List[Tuple[float, str, object]] = []
        for a in all_links:
            try:
                bb = await a.bounding_box()
                if not bb:
                    continue
                href = await a.get_attribute("href")
                if not href:
                    continue
                url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                link_triplets.append((bb["y"], url, a))
            except:
                continue

        # y 오름차순 정렬
        link_triplets.sort(key=lambda t: t[0])
        ys = [y for y, _, _ in link_triplets]
        cutoff_y = _first_big_gap_cutoff(ys, min_gap=140.0)  # 첫 큰 갭 기준으로 상단/하단 분리

        # 상단(=베스트 셀러 영역)만 선택
        top_links: List[Tuple[str, object]] = []
        seen = set()
        for y, url, a in link_triplets:
            if y >= cutoff_y:
                break
            if url in seen:
                continue
            seen.add(url)
            top_links.append((url, a))

        print(f"🔎 링크 전체: {len(link_triplets)}개, 상단 선택: {len(top_links)}개, 컷오프 y={cutoff_y:.1f}")

        items: List[Dict] = []
        rank = 0
        parsed_ok = 0

        for url, a in top_links:
            try:
                # 근접 카드 컨테이너
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

                # 가격 블롭: 텍스트+HTML+모든 속성까지
                price_blob = await _gather_price_blob(card)
                price_info = parse_prices_and_discount(price_blob)

                if price_info.get("price_current_usd") is None:
                    continue  # 가격 없는 배너/기획전

                rank += 1
                parsed_ok += 1
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
                    "product_url": url,
                    "image_url": img_url
                })
            except Exception:
                continue

        await context.close()
        print(f"✅ 가격 파싱 성공: {parsed_ok}개")

        items.sort(key=lambda x: x["rank"])
        return items
