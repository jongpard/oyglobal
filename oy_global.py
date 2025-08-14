# oy_global.py
import asyncio
from typing import List, Dict

from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

JS_EXTRACT = r"""
() => {
  const asNum = (s) => {
    if (!s) return null;
    const m = String(s).match(/([\d,]+(?:\.\d{2})?)/);
    if (!m) return null;
    return parseFloat(m[1].replace(/,/g, ""));
  };
  const inRange = (v) => typeof v === "number" && v >= 0.5 && v <= 500;

  // -------------------- 트렌딩 섹션 y-range --------------------
  let trendTop = Number.NEGATIVE_INFINITY;
  let trendBottom = Number.NEGATIVE_INFINITY;

  // 헤더 텍스트를 가진 최소 노드 찾기
  const candidates = Array.from(document.querySelectorAll("body *"))
    .filter(el => /what.?s trending in korea/i.test(el.textContent || ""));

  if (candidates.length) {
    // 가장 위에 있는(작은 y) 노드 기준
    candidates.sort((a,b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
    const head = candidates[0];

    // 헤더에 가장 가까운 컨테이너(상품들을 실제로 담는 박스)를 찾되,
    // y-범위가 너무 크면 강제로 캡(최대 1400px)
    let cont = head.closest("section,div,article") || head;
    let r = cont.getBoundingClientRect();
    let top = r.top + window.scrollY;
    let bottom = r.bottom + window.scrollY;

    // 과하게 큰 컨테이너(페이지 전체를 덮는 경우) 방지
    if (bottom - top > 1400) bottom = top + 1400;

    trendTop = top;
    trendBottom = bottom;
  }

  // -------------------- 카드 스캔 --------------------
  const anchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const seen = new Set();
  const rows = [];

  for (const a of anchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest("li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item") || a;
    const rect = card.getBoundingClientRect();
    const yAbs = rect.top + window.scrollY;

    // 트렌딩 영역 제외
    if (Number.isFinite(trendTop) && Number.isFinite(trendBottom)) {
      if (yAbs >= trendTop && yAbs <= trendBottom) continue;
    }

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = (brandEl.textContent || "").trim();

    // 상품명 (a의 title/aria → 명칭 셀렉터 → img alt → a 텍스트)
    let name = a.getAttribute("title") || a.getAttribute("aria-label") || "";
    if (!name || name.length < 3) {
      const nameEl = card.querySelector("p.name, .name, .prd_name, .product-name, strong.name");
      if (nameEl) name = (nameEl.textContent || "");
    }
    if (!name || name.length < 3) {
      const altEl = card.querySelector("img[alt]");
      if (altEl) name = altEl.getAttribute("alt") || "";
    }
    if (!name || name.length < 3) {
      name = a.textContent || "";
    }
    name = (name || "").replace(/\s+/g, " ").trim();
    if (!name) name = "상품";

    // 이미지
    let img = "";
    const imgEl = card.querySelector("img");
    if (imgEl) img = imgEl.src || imgEl.getAttribute("src") || "";

    // -------------------- 가격 추출 --------------------
    // 1) price 관련 요소를 우선
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();

    // 2) 없으면 카드 전체 visible 텍스트
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const dollars = [];

    // US$xx.xx 혹은 US$xx
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]);
      if (v != null) dollars.push(v);
    }
    // 소수 둘째 자리
    for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
      const v = asNum(m[0]);
      if (v != null) dollars.push(v);
    }
    // 정수 금액 (뒤에 .00 표시가 CSS로 처리되는 경우)
    for (const m of priceText.matchAll(/\b(\d{1,3}(?:,\d{3})*)\b/g)) {
      const v = asNum(m[0]);
      if (v != null && Number.isInteger(v)) dollars.push(v);
    }

    // Value: US$xx.xx → 정가 힌트
    let valuePrice = null;
    const vm = priceText.match(/(?<![A-Za-z0-9_])value(?!\s*=)\s*[:：]?\s*US\$ ?([\d,]+(?:\.\d{2})?)/i);
    if (vm) valuePrice = asNum(vm[0]);

    const clean = dollars.filter(inRange);
    if (clean.length === 0) continue;

    const priceCur = Math.min(...clean);
    const priceOri = (valuePrice && inRange(valuePrice))
      ? valuePrice
      : (clean.length >= 2 ? Math.max(...clean) : priceCur);

    rows.push({
      y: yAbs,
      brand: brand || null,
      product_name: name || "상품",
      price_current_usd: priceCur,
      price_original_usd: priceOri,
      product_url: abs,
      image_url: img || null,
    });
    seen.add(abs);
  }

  // 위→아래 정렬, 최대 100개
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  return {
    anchorCount: anchors.length,
    candidateCount: rows.length,
    picked: items.length,
    trendTop,
    trendBottom,
    items,
  };
}
"""

async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120 Safari/537.36"),
            locale="en-US",
        )
        page = await context.new_page()
        await page.goto(BEST_URL, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_load_state("networkidle")

        # 끝까지 스크롤 (지연 로딩 안정화)
        let_prev = -1
        same = 0
        for i in range(40):
            await page.mouse.wheel(0, 3200)
            await asyncio.sleep(0.7)
            cnt = await page.locator("a[href*='product/detail']").count()
            if cnt == let_prev:
                same += 1
            else:
                same = 0
            let_prev = cnt
            if i >= 14 and same >= 3:
                break

        # 페이지 내부 추출
        res = await page.evaluate(JS_EXTRACT)
        await context.close()

    print(f"🔎 앵커 수: {res.get('anchorCount')}, 후보 카드: {res.get('candidateCount')}, 최종 채택: {res.get('picked')}")
    const_top, const_bottom = res.get("trendTop"), res.get("trendBottom")
    if Number.isFinite(const_top) && Number.isFinite(const_bottom):
        print(f"🧭 트렌딩 y=({const_top:.1f}~{const_bottom:.1f})")

    items: List[Dict] = res.get("items", [])

    # 날짜/할인율/플래그
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = False

    return items
