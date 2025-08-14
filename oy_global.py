import re
from typing import Dict, List, Any
from datetime import datetime, timezone, timedelta

from playwright.async_api import async_playwright, TimeoutError as PwTimeout


BASE = "https://global.oliveyoung.com"


STOPWORDS = {
    "HOT DEAL",
    "BEST",
    "NEW",
    "1+1",
    "GIFT",
    "ONLY",
    "EXCLUSIVE",
    "OLIVE YOUNG ONLY",
    "SLOW AGING",
}


def _parse_usd(text: str) -> str:
    """
    'US$25.99' -> '25.99'
    'Value: US$86.00' -> '86.00'
    아무것도 못 찾으면 '' 반환
    """
    if not text:
        return ""
    m = re.search(r"US?\$\s*([0-9][0-9,]*\.?[0-9]*)", text.replace(",", ""))
    return m.group(1) if m else ""


def _pct(cur: str, orig: str) -> str:
    try:
        c = float(cur)
        o = float(orig)
        if o <= 0:
            return ""
        p = round((1.0 - c / o) * 100.0, 2)
        # 음수 할인(즉 인상)은 표시 안 함
        if p < 0:
            return ""
        # 소수점 .0 제거
        return f"{p:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return ""


def _abs_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return BASE + url
    # 상대라면 일단 붙이기
    return BASE + "/" + url


def _clean_brand(brand: str) -> str:
    if not brand:
        return ""
    b = brand.strip()
    if b.upper() in STOPWORDS:
        return ""
    # 너무 긴 건 제품명이 섞였을 가능성 -> 브랜드만 남기도록 긴 공백 기준 첫 토큰만
    if len(b) > 60 and " " in b:
        b = b.split(" ")[0]
    return b


async def _load_page(page):
    await page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
    # Top Orders 구간이 보이는 곳까지 스크롤
    try:
        await page.wait_for_selector("text=/Top Orders/i", timeout=5000)
    except PwTimeout:
        pass

    # Lazy load를 위해 충분히 아래로 스크롤
    last = 0
    for _ in range(20):
        await page.mouse.wheel(0, 1800)
        await page.wait_for_timeout(500)
        y = await page.evaluate("() => window.scrollY")
        if y == last:
            break
        last = y


async def _find_trending_cut(page) -> float:
    """
    'What's trending in Korea?' 헤더의 문서 Y좌표를 찾는다.
    못 찾으면 매우 큰 값(무한대 느낌)을 반환하여 필터가 동작하지 않도록 함.
    """
    candidates = [
        r"/What.?s trending in Korea/i",   # what's / what’s 대응
        r"/Trending in Korea/i",
    ]
    for pat in candidates:
        try:
            loc = page.locator(f"text={pat}").first
            await loc.wait_for(state="visible", timeout=1500)
            box = await loc.bounding_box()
            if box:
                return box["y"]  # 문서 좌표
        except PwTimeout:
            continue
        except Exception:
            continue
    return float("inf")


async def _collect_cards(page, trending_cut_y: float) -> List[Dict[str, Any]]:
    """
    카드에서 정보 추출.
    - 트렌딩 섹션 위쪽(y < trending_cut_y)만 수집
    - 부족하면 DOM 순서대로 100개까지 자르고 반환
    """
    # 카드의 핵심 앵커
    sel = "a[href*='product/detail']"

    # 브라우저 컨텍스트에서 한 번에 정보 모으기(성능)
    js = """
    (nodes, stopY) => {
      const STOP = new Set([
        "HOT DEAL","BEST","NEW","1+1","GIFT","ONLY","EXCLUSIVE","OLIVE YOUNG ONLY","SLOW AGING"
      ]);

      function pickBrand(card) {
        // 흔한 브랜드 셀렉터들
        const cand = card.querySelector("span.brand, .brand, .prd_brand, .product-brand, .txt_brand, .info_brand");
        if (cand) {
          const t = cand.textContent.trim();
          if (t && !STOP.has(t.toUpperCase())) return t;
        }
        // 여러 span 중 첫 번째 '정상' 텍스트 골라보기
        const spans = card.querySelectorAll("span, em, strong");
        for (const s of spans) {
          const t = (s.textContent || "").trim();
          if (!t) continue;
          const upper = t.toUpperCase();
          if (STOP.has(upper)) continue;
          // 숫자/가격/퍼센트 느낌 제거
          if (/[0-9$%]/.test(t)) continue;
          // 너무 길면 제품명일 가능성
          if (t.length > 35) continue;
          // 이 정도면 브랜드로 보자
          return t;
        }
        return "";
      }

      function text(el) { return (el && el.textContent || "").trim(); }

      return nodes.map(a => {
        const card = a.closest("li") || a.closest("div");
        if (!card) return null;

        const rect = card.getBoundingClientRect();
        const pageY = rect.top + window.scrollY;

        // 제품명
        let nameEl = card.querySelector("p.name, .name, .product-name, .prd_name, .txt_name");
        let product = text(nameEl) || text(a);

        // 이미지
        const imgEl = card.querySelector("img");
        const img = imgEl ? (imgEl.getAttribute("src") || imgEl.getAttribute("data-src") || "") : "";

        // 가격
        const curEl = card.querySelector(".price strong, .sale-price, .now, .price .num, .product-price, .txt_price strong, .txt_price .num");
        const origEl = card.querySelector(".price del, del, .origin, .before, .strike, .txt_price del");
        const valEl  = card.querySelector(".value, .benefit, .value-price, .txt_benefit");

        // 랭크 뱃지(있으면 참고, 없으면 나중에 파이썬에서 1~100 부여)
        const numEl = card.querySelector(".rank, .num, .badge, .number");
        const rank = numEl ? parseInt((numEl.textContent||'').replace(/[^0-9]/g,'')) : null;

        return {
          y: pageY,
          rank,
          brand: pickBrand(card),
          product,
          curText: text(curEl),
          origText: text(origEl),
          valText: text(valEl),
          url: a.getAttribute("href") || "",
          img,
        };
      })
      .filter(x => !!x && x.product && x.url)
      .filter(x => x.y < stopY)  // 트렌딩 컷
    }
    """
    nodes = await page.eval_on_selector_all(sel, js, trending_cut_y)

    # 트렌딩 컷이 무한대(=못 찾음)인 경우: 페이지 전체 중 앞에서 100개만 사용
    if trending_cut_y == float("inf"):
        nodes = nodes[:130]  # 여유로 모아두고 100개만 자르기

    results: List[Dict[str, Any]] = []
    seen = set()
    for n in nodes:
        url = _abs_url(n.get("url", ""))
        if url in seen:
            continue
        seen.add(url)

        brand = _clean_brand(n.get("brand", ""))
        product = (n.get("product") or "").strip()
        # 가격 파싱
        cur = _parse_usd(n.get("curText", ""))
        orig = _parse_usd(n.get("origText", ""))
        val = _parse_usd(n.get("valText", ""))
        has_value = "TRUE" if val else "FALSE"
        disc = _pct(cur, orig) if cur and orig else ""

        results.append(
            {
                "date_kst": "",  # main에서 세팅
                "rank": n.get("rank") or "",
                "brand": brand,
                "product_name": product,
                "price_current_usd": cur,
                "price_original_usd": orig,
                "discount_rate_pct": disc,
                "value_price_usd": val,
                "has_value_price": has_value,
                "product_url": url,
                "image_url": n.get("img", ""),
            }
        )

    # 100개로 한정
    return results[:100]


async def scrape_oliveyoung_global() -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        await _load_page(page)
        cut_y = await _find_trending_cut(page)
        items = await _collect_cards(page, trending_cut_y=cut_y)

        await context.close()
        await browser.close()

        return items
