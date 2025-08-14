# oy_global.py
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
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"),
        locale="en-US"
    )
    return context

async def _try_close_overlays(page):
    for sel in [
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "button:has-text('OK')",
        "button:has-text('Close')",
        ".cookie .close", ".popup .close", ".modal .btn-close",
    ]:
        try:
            if await page.is_visible(sel):
                await page.click(sel, timeout=600)
                await asyncio.sleep(0.2)
        except:
            pass

async def _wait_attached(page, selector, timeout=22000) -> bool:
    try:
        await page.wait_for_selector(selector, state="attached", timeout=timeout)
        return True
    except:
        return False

async def _scroll_until_stable(page, min_iters=12, max_iters=32, step=3200, pause=0.75):
    prev = -1
    stable = 0
    for i in range(max_iters):
        await page.mouse.wheel(0, step)
        await asyncio.sleep(pause)
        cnt = len(await page.query_selector_all("a[href*='product/detail']"))
        if cnt == prev:
            stable += 1
        else:
            stable = 0
        prev = cnt
        if i >= min_iters and stable >= 3:
            break

async def _find_trending_bounds(page) -> Tuple[float, float]:
    heads = []
    for txt in ["What's trending in Korea?", "What’s trending in Korea?"]:
        try:
            loc = page.get_by_text(txt).first
            bb = await loc.bounding_box()
            if bb: heads.append(bb["y"])
        except:
            pass
    if not heads:
        return float("inf"), float("-inf")
    top_y = min(heads)
    bottom_y = top_y + 1800.0
    return top_y, bottom_y

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

async def _extract_name(a, card) -> str:
    # 0) a 속성 우선
    for attr in ["aria-label", "title"]:
        v = await a.get_attribute(attr)
        if v:
            v = _clean(v)
            if v: return v
    # 1) 전용 셀렉터
    for sel in ["p.name", ".name", ".prd_name", ".product-name", "strong.name"]:
        el = await card.query_selector(sel)
        if el:
            t = _clean(await el.inner_text())
            if t: return t
    # 2) 앵커 텍스트
    t = _clean(await a.inner_text())
    if len(t) >= 3:
        return t
    # 3) 이미지 alt
    img = await card.query_selector("img[alt]")
    if img:
        alt = _clean(await img.get_attribute("alt"))
        if alt: return alt
    # 4) 카드 title/aria-label
    for attr in ["title", "aria-label"]:
        val = await card.get_attribute(attr)
        if val:
            val = _clean(val)
            if val: return val
    return "상품"

async def _gather_price_text(card) -> str:
    try:
        txt = await card.evaluate("el => (el.innerText || '').replace(/\\n/g, ' ')")
        html = await card.inner_html()
        return f"{txt} {html}"
    except:
        try:
            return await card.inner_text()
        except:
            return ""

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        context = await _new_context(pw)
        page = await context.new_page()

        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await _try_close_overlays(page)
        await page.wait_for_load_state("networkidle")

        await _wait_attached(page, "a[href*='product/detail']", timeout=25000)
        await _scroll_until_stable(page)

        # 모든 상세 링크(y좌표 포함)
        all_links = await page.query_selector_all("a[href*='product/detail']")
        triples: List[Tuple[float, str, object]] = []
        for a in all_links:
            try:
                bb = await a.bounding_box()
                if not bb: continue
                href = await a.get_attribute("href")
                if not href: continue
                url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"
                triples.append((bb["y"], url, a))
            except:
                continue

        triples.sort(key=lambda t: t[0])
        t_top, t_bottom = await _find_trending_bounds(page)

        # 트렌딩 제외(위+아래 모두 포함)
        selected: List[Tuple[str, object]] = []
        seen = set()
        for y, url, a in triples:
            if t_top <= y <= t_bottom:
                continue
            if url in seen:
                continue
            seen.add(url)
            selected.append((url, a))

        print(f"🔎 링크 전체: {len(triples)}개, 선택(트렌딩 제외): {len(selected)}개, "
              f"트렌딩 y=({t_top:.1f}~{t_bottom:.1f})")

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
                        t = _clean(await el.inner_text())
                        if t:
                            brand = t
                            break

                name = await _extract_name(a, card)
                price_blob = await _gather_price_text(card)
                price_info = parse_prices_and_discount(price_blob)
                if price_info.get("price_current_usd") is None:
                    continue

                rank += 1
                parsed_ok += 1
                items.append({
                    "date_kst": kst_today_str(),
                    "rank": rank,
                    "brand": brand,
                    "product_name": name,
                    "price_current_usd": price_info["price_current_usd"],
                    "price_original_usd": price_info["price_original_usd"],
                    "discount_rate_pct": price_info["discount_rate_pct"],
                    "value_price_usd": price_info["value_price_usd"],
                    "has_value_price": price_info["has_value_price"],
                    "product_url": url,
                    "image_url": img_url,
                })
            except Exception:
                continue

        await context.close()
        print(f"✅ 가격 파싱 성공: {parsed_ok}개")
        items.sort(key=lambda x: x["rank"])
        return items
