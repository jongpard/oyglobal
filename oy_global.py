import asyncio
import re
from typing import List, Dict, Optional

from playwright.async_api import async_playwright

BASE_URL = "https://global.oliveyoung.com"

# 랭킹(Top Orders) 페이지 URL — 기존에 쓰던 주소로 그대로 두고 필요하면 여기만 바꿔줘
BEST_URL = f"{BASE_URL}/category/best-seller"

PRICE_RE = re.compile(r"US?\$[\s]*([0-9]+(?:\.[0-9]+)?)")
NUM_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")

def _to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = NUM_RE.search(text.replace(",", ""))
    return float(m.group(1)) if m else None

async def _get_text(locator):
    try:
        t = (await locator.inner_text()).strip()
        return re.sub(r"\s+", " ", t)
    except:
        return ""

async def _first_text(node, selectors: List[str]) -> str:
    for sel in selectors:
        loc = node.locator(sel).first
        try:
            if await loc.count() > 0:
                t = await _get_text(loc)
                if t:
                    return t
        except:
            pass
    return ""

def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text.replace(",", ""))
    if not m:
        return _to_float(text)
    return float(m.group(1))

def _normalize_slack_line(brand: str, product: str) -> str:
    # 제품명이 브랜드로 시작하면(중복) 브랜드는 생략
    if product.lower().startswith((brand or "").lower().strip()):
        return product.strip()
    return f"{brand.strip()} {product.strip()}".strip()

async def _scroll_until(page, target_locator, need_count=100, max_loops=60):
    """무한 스크롤 지원: target_locator의 개수가 need_count에 도달할 때까지 스크롤."""
    for _ in range(max_loops):
        cnt = await target_locator.count()
        if cnt >= need_count:
            return
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await page.wait_for_timeout(900)

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
        )
        page = await ctx.new_page()
        await page.goto(BEST_URL, wait_until="domcontentloaded")

        # 1) 'Top Orders' 섹션 컨테이너만 잡는다 (상단 배너/카테고리/트렌딩 제외)
        #    다양한 마크업 변화에 대응하기 위해 헤더 텍스트 기준으로 섹션을 추적한다.
        heading = page.locator("text=/Top Orders/i").first
        if await heading.count() == 0:
            # 영어 사이트 변형 대비 ('Best Sellers'로만 노출되는 케이스)
            heading = page.locator("text=/Best Sellers|Top Orders/i").first
        await heading.wait_for(state="visible", timeout=30000)

        # 헤더의 가장 가까운 섹션/랩퍼를 기준으로 카드만 선택
        top_section = heading.locator("xpath=ancestor::section[1]")
        if await top_section.count() == 0:
            top_section = heading.locator("xpath=ancestor::*[self::section or self::div][1]")

        # 섹션 내부의 '상품 카드'만: a[href*='product/detail'] 를 갖고 있는 카드 래퍼를 기준으로 수집
        # (상단 배너/카테고리는 이 섹션 외부라 자연스럽게 제외됨)
        card_link = top_section.locator("a[href*='product/detail']")
        # 스크롤하며 100개 이상 로드
        await _scroll_until(page, card_link, need_count=100, max_loops=80)

        # 실제 카드 래퍼(제품 정보가 함께 있는 박스)로 범위를 좁힌다
        # a 태그에서 상위 카드로 올라가 정보 추출
        # (li, div 등 마크업이 바뀌어도 'a'를 기준으로 한 단계 위에서 찾는다)
        anchors = await card_link.element_handles()

        items = []
        seen = set()
        rank = 1

        for a in anchors:
            # 카드 래퍼
            card = await a.evaluate_handle("node => node.closest('li, .prd, .product, .card, .product-item, .item') || node")
            # 링크/이미지
            href = await a.get_attribute("href") or ""
            if "product/detail" not in href:
                continue
            # 절대 URL로
            if href.startswith("/"):
                product_url = BASE_URL + href
            elif href.startswith("http"):
                product_url = href
            else:
                product_url = BASE_URL + "/" + href.lstrip("/")

            if product_url in seen:
                continue
            seen.add(product_url)

            # 브랜드/제품명
            brand = await _first_text(
                card,
                [".brand", ".prd_brand", ".product-brand", ".txt_brand", "strong.brand", "span.brand"]
            )
            product_name = await _first_text(
                card,
                [".name", ".prd_name", ".product-name", ".txt_name", "a.name", "a.product-name"]
            )
            # Fallback: 링크 텍스트/이미지 alt
            if not product_name:
                product_name = await _first_text(a, [":scope"])
            if not product_name:
                img_alt = await _first_text(card, ["img[alt]"])
                product_name = img_alt or ""

            # 가격
            # 현재가(세일가)
            cur_price_text = await _first_text(
                card,
                [".price .sale, .price .current, .price .now, .product-price-now, .product-price"]
            )
            if not cur_price_text:
                cur_price_text = await _first_text(card, [".price", ".prd_price", ".txt_price"])
            price_current = _parse_price(cur_price_text)

            # 정가
            orig_price_text = await _first_text(
                card,
                [".price del", ".price .original", ".product-price-was", ".origin", "del"]
            )
            price_original = _parse_price(orig_price_text)

            # 'Value price'(번들/구성의 명시 값) – 있으면 파싱
            value_price_text = await _first_text(
                card,
                [".value, .benefit, .txt_value, .value-price"]
            )
            value_price = _parse_price(value_price_text)
            has_value_price = bool(value_price is not None)

            # 할인율 (카드에 숫자로 있는 경우 우선)
            pct_text = await _first_text(
                card,
                [".discount, .sale-percent, .percent, .pct"]
            )
            discount_pct = _to_float(pct_text)

            # 계산 보정
            if discount_pct is None and price_current and price_original:
                try:
                    discount_pct = round((1 - (price_current / price_original)) * 100, 2)
                except ZeroDivisionError:
                    discount_pct = None

            # 이미지
            img_url = await _first_text(card, ["img[src]", "img[data-src]"])
            if img_url and not img_url.startswith("http"):
                img_url = BASE_URL + "/" + img_url.lstrip("/")

            # 브랜드-제품 중복 정리 (브랜드만 남기고 제품명은 순수 제품명)
            brand = brand.strip()
            product_name = product_name.strip()

            items.append({
                "date_kst": None,  # 저장 전에 main에서 채움
                "rank": rank,
                "brand": brand,
                "product_name": product_name,
                "price_current_usd": price_current,
                "price_original_usd": price_original,
                "discount_rate_pct": discount_pct,
                "value_price_usd": value_price,
                "has_value_price": str(has_value_price).upper(),
                "product_url": product_url,
                "image_url": img_url,
            })
            rank += 1
            if rank > 100:
                break

        # 날짜 채우기
        from datetime import datetime, timezone, timedelta
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).strftime("%Y-%m-%d")
        for it in items:
            it["date_kst"] = today

        return items

def build_top10_slack_text(df) -> str:
    """Slack 용 TOP 10 메시지 포맷. 제품명 하이퍼링크, 브랜드-제품 중복 자동 제거."""
    lines = []
    header = f"*올리브영 글로벌 전체 랭킹* ({df.iloc[0]['date_kst']} KST)\n*TOP 10*"
    lines.append(header)

    for _, row in df.iterrows():
        rank = int(row["rank"])
        brand = (row["brand"] or "").strip()
        name = (row["product_name"] or "").strip()
        cur_p = row.get("price_current_usd")
        org_p = row.get("price_original_usd")
        pct = row.get("discount_rate_pct")
        url = row.get("product_url")

        # 브랜드-제품 중복 제거
        title = _normalize_slack_line(brand, name)
        link = f"<{url}|{title}>"

        tail = []
        if cur_p is not None:
            tail.append(f"US${cur_p:.2f}")
        if org_p is not None:
            tail.append(f"(정가 US${org_p:.2f})")
        if pct is not None:
            tail.append(f"(↓{pct:.2f}%)")

        line = f"{rank}. {link} – " + " ".join(tail) if tail else f"{rank}. {link}"
        lines.append(line)

    return "\n".join(lines)
