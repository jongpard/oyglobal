import asyncio
import re
from typing import List, Dict, Tuple

from playwright.async_api import async_playwright, BrowserContext
from price_parser import parse_prices_and_discount
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

async def _new_context(pw) -> BrowserContext:
    browser = await pw.chromium.launch(headless=True)
    context = await pw.chromium.launch(headless=True)
    # 위 한 줄 오타 방지: 실제 context는 아래에서 생성
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
    # 'What's trending in Korea?' 헤더의 Y좌표. 찾지 못하면 매우 큰 값 반환(=컷 안함)
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

def _extract_prdtno(url: str) -> str:
    m = re.search(r"prdtNo=([A-Za-z0-9]+)", url)
    return m.group(1) if m else ""

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
        # 실패 시 텍스트만
        try:
            return await card_handle.evaluate("(el)=> (el.innerText||'') + ' ' + (el.innerHTML||'')")
        except:
            return ""

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        # 루트 찾기(있으면)
        root = None
        for sel in ["#pillsTab1Nav1", "[id*='pillsTab1']", "section [data-list='best-seller']"]:
            if await _wait_attached(page, sel, timeout=7000):
                root = sel
                break

        # 제품 링크 DOM 부착 대기
        await _wait_attached(page, "a[href*='product/detail']", timeout=15000)

        # 충분히 스크롤 (지연 로딩 대비)
        for _ in range(14):
            await page.mouse.wheel(0, 2800)
            await asyncio.sleep(0.9)

        trending_y = await _get_trending_boundary_y(page)
        scope_prefix = f"{root} " if root else ""
        links = await page.query_selector_all(f"{scope_prefix}a[href*='product/detail']")

        # 링크 정리: 트렌딩 섹션 위쪽만, y 오름차순, URL 중복 제거
        link_info: List[Tuple[float, str, object]] = []
        for a in links:
            try:
                bb = await a.bounding_box()
                if bb and bb["y"] >= trending_y:
                    continue  # 트렌딩 이하 컷
                href = await a.get_attribute("href")
                if not href:
                    continue
                url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                link_info.append((bb["y"] if bb else 0.0, url, a))
            except:
                continue

        link_info.sort(key=lambda t: t[0])
        unique_urls = []
        seen = set()
        for _, url, a in link_info:
            if url in seen:
                continue
            seen.add(url)
            unique_urls.append((url, a))

        print(f"🔎 링크(트렌딩 위) 감지: {len(unique_urls)}개")

        items: List[Dict] = []
        rank = 0
        parsed_ok = 0

        for url, a in unique_urls:
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
