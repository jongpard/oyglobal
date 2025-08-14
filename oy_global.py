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

  // 트렌딩 섹션 y-range
  let trendTop = -Infinity, trendBottom = -Infinity;
  const allNodes = Array.from(document.querySelectorAll("body *"));
  const trendNode = allNodes.find(el => /what.?s trending in korea/i.test(el.textContent || ""));
  if (trendNode) {
    const box = (trendNode.closest("section,div,article") || trendNode).getBoundingClientRect();
    trendTop = box.top + window.scrollY;
    trendBottom = box.bottom + window.scrollY;
  }

  // 상세 링크 모으기
  const anchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const seen = new Set();
  const candidates = [];

  for (const a of anchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest("li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item") || a;
    const rect = card.getBoundingClientRect();
    const yAbs = rect.top + window.scrollY;

    // 트렌딩 제외
    if (trendTop > -Infinity && yAbs >= trendTop && yAbs <= trendBottom) continue;

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = (brandEl.textContent || "").trim();

    // 상품명(우선순위: a title/aria → 명칭 셀렉터 → img alt → a 텍스트)
    let name = a.getAttribute("title") || a.getAttribute("aria-label") || "";
    if (!name || name.length < 3) {
      const nameEl = card.querySelector("p.name, .name, .prd_name, .product-name, strong.name");
      if (nameEl) name = (nameEl.textContent || "");
    }
    if (!name || name.length < 3) {
      const imgAlt = card.querySelector("img[alt]");
      if (imgAlt) name = imgAlt.getAttribute("alt") || "";
    }
    if (!name || name.length < 3) {
      name = a.textContent || "";
    }
    name = name.replace(/\s+/g, " ").trim();
    if (!name) name = "상품";

    // 이미지
    let img = "";
    const imgEl = card.querySelector("img");
    if (imgEl) img = imgEl.src || imgEl.getAttribute("src") || "";

    // ----- 가격 추출 -----
    // 1) price 라는 단어가 class/id에 포함된 요소들을 먼저 긁기
    const priceBlocks = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    ));
    let priceText = priceBlocks.map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();

    // 2) 보조: price 영역이 비어있으면 카드 전체 텍스트 사용
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const dollars = [];
    // US$xx.xx / US$xx  모두 수집
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      dollars.push(asNum(m[0]));
    }
    // 소수 둘째자리 가격(기호 없는 케이스)
    for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
      dollars.push(asNum(m[0]));
    }
    // 정수만 있는 가격도 (뒤에 .00 붙는 표시일 수 있어서)
    for (const m of priceText.matchAll(/\b(\d{1,3}(?:,\d{3})*)\b/g)) {
      const v = asNum(m[0]);
      if (v != null && Number.isInteger(v) && v >= 1 && v <= 500) dollars.push(v);
    }

    // Value: US$xx.xx → 정가 힌트
    let valuePrice = null;
    const vm = priceText.match(/(?<![A-Za-z0-9_])value(?!\s*=)\s*[:：]?\s*US\$ ?([\d,]+(?:\.\d{2})?)/i);
    if (vm) valuePrice = asNum(vm[0]);

    // 범위 필터
    const clean = dollars.filter(inRange);
    if (clean.length === 0) continue;

    // 현재가/정가 결정
    const priceCur = Math.min(...clean);
    const priceOri = valuePrice && inRange(valuePrice)
      ? valuePrice
      : (clean.length >= 2 ? Math.max(...clean) : priceCur);

    candidates.push({
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

  // 위→아래 정렬 후 100개 제한
  candidates.sort((a, b) => a.y - b.y);
  const items = candidates.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));
  return {
    anchorCount: anchors.length,
    candidateCount: candidates.length,
    picked: items.length,
    trendTop,
    trendBottom,
    items
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

        # 지연로딩 대비: 스크롤을 안정될 때까지 반복
        prev = -1
        same = 0
        for i in range(40):
            await page.mouse.wheel(0, 3200)
            await asyncio.sleep(0.7)
            cnt = await page.locator("a[href*='product/detail']").count()
            if cnt == prev:
                same += 1
            else:
                same = 0
            prev = cnt
            if i >= 14 and same >= 3:
                break

        # 페이지 내부에서 한 번에 추출 (디버그 카운트 포함)
        res = await page.evaluate(JS_EXTRACT)
        await context.close()

    print(f"🔎 앵커 수: {res.get('anchorCount')}, 후보 카드: {res.get('candidateCount')}, 최종 채택: {res.get('picked')}")
    tt, tb = res.get("trendTop"), res.get("trendBottom")
    if tt not in (None, float("inf")) and tb not in (None, float("-inf")):
        print(f"🧭 트렌딩 y=({tt:.1f}~{tb:.1f})")

    rows: List[Dict] = res.get("items", [])
    # 파이썬 쪽 후처리(날짜/할인율/플래그)
    for r in rows:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = False

    return rows
