# oy_global.py
import asyncio
import math
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

  // -------- 트렌딩 섹션 컨테이너 정확 탐지 --------
  let trendingContainer = null;
  (function findTrendingContainer(){
    const heads = Array.from(document.querySelectorAll("body *"))
      .filter(el => /what.?s trending in korea/i.test((el.textContent || "").trim()));
    if (!heads.length) return;

    // 가장 위에 있는 헤더 기준
    heads.sort((a,b)=>a.getBoundingClientRect().top - b.getBoundingClientRect().top);
    const head = heads[0];

    // 헤더에서 위로 올라가며 product/detail 링크를 '충분히' 포함하는 가장 가까운 조상 선택
    const MIN_LINKS = 4;    // 섹션이라고 부를 최소 링크 수
    const MAX_LINKS = 60;   // 너무 커지면 전체 페이지일 수 있으니 상한
    let node = head;
    for (let i=0; i<8 && node; i++, node = node.parentElement) {
      const cnt = node.querySelectorAll("a[href*='product/detail']").length;
      if (cnt >= MIN_LINKS && cnt <= MAX_LINKS) {
        trendingContainer = node;
        break;
      }
    }
    // 못 찾으면 헤더 바로 위 컨테이너 시도
    if (!trendingContainer) trendingContainer = head.closest("section,div,article") || head.parentElement || head;
  })();

  // -------- 베스트셀러 카드 스캔 --------
  const anchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const seen = new Set();
  const rows = [];

  for (const a of anchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest("li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item") || a;

    // 트렌딩 섹션 내부 카드는 제외
    if (trendingContainer && trendingContainer.contains(card)) continue;

    const rect = card.getBoundingClientRect();
    const yAbs = rect.top + window.scrollY;

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = (brandEl.textContent || "").trim();

    // 상품명
    let name = a.getAttribute("title") || a.getAttribute("aria-label") || "";
    if (!name || name.length < 3) {
      const nameEl = card.querySelector("p.name, .name, .prd_name, .product-name, strong.name");
      if (nameEl) name = (nameEl.textContent || "");
    }
    if (!name || name.length < 3) {
      const altEl = card.querySelector("img[alt]");
      if (altEl) name = altEl.getAttribute("alt") || "";
    }
    if (!name || name.length < 3) name = a.textContent || "";
    name = (name || "").replace(/\s+/g, " ").trim();
    if (!name) name = "상품";

    // 이미지
    let img = "";
    const imgEl = card.querySelector("img");
    if (imgEl) img = imgEl.src || imgEl.getAttribute("src") || "";

    // ---------- 가격 ----------
    // 1) price 관련 요소의 보이는 텍스트 우선
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    // 2) 없으면 카드 전체 텍스트
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const amounts = [];
    // (A) US$ 붙은 금액(우선)
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) amounts.push(v);
    }
    // (B) A가 비어있을 때만, 소수 둘째자리 금액 보조
    if (amounts.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) amounts.push(v);
      }
    }
    // 정수 금액은 사용하지 않음

    // Value: US$xx.xx → 정가 힌트
    let valuePrice = null;
    const vm = priceText.match(/(?<![A-Za-z0-9_])value(?!\s*=)\s*[:：]?\s*US\$ ?([\d,]+(?:\.\d{2})?)/i);
    if (vm) valuePrice = asNum(vm[0]);

    const clean = amounts.filter(inRange);
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
      value_price_usd: valuePrice || null,
      product_url: abs,
      image_url: img || null,
    });
    seen.add(abs);
  }

  // 위→아래 정렬, 100개 제한
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  // 디버그: 트렌딩 컨테이너 대략 정보
  let trendInfo = null;
  if (trendingContainer) {
    const r = trendingContainer.getBoundingClientRect();
    trendInfo = { top: r.top + window.scrollY, bottom: r.bottom + window.scrollY, links: trendingContainer.querySelectorAll("a[href*='product/detail']").length };
  }

  return {
    anchorCount: anchors.length,
    candidateCount: rows.length,
    picked: items.length,
    trendInfo,
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

        # 끝까지 스크롤(지연 로딩 안정화)
        prev = -1
        same = 0
        for i in range(40):
            await page.mouse.wheel(0, 3200)
            await asyncio.sleep(0.7)
            cnt = await page.locator("a[href*='product/detail']").count()
            if cnt == prev: same += 1
            else: same = 0
            prev = cnt
            if i >= 14 and same >= 3:
                break

        res = await page.evaluate(JS_EXTRACT)
        await context.close()

    print(f"🔎 앵커 수: {res.get('anchorCount')}, 후보 카드: {res.get('candidateCount')}, 최종 채택: {res.get('picked')}")
    ti = res.get("trendInfo")
    if isinstance(ti, dict):
        print(f"🧭 트렌딩 섹션: links={ti.get('links')}, y=({ti.get('top'):.1f}~{ti.get('bottom'):.1f})")

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))

    return items
