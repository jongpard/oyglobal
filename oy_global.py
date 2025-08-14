# oy_global.py
import asyncio
import math
from typing import List, Dict
from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# 페이지 안에서 카드들을 "보이는 그대로" 추출
JS_EXTRACT = r"""
() => {
  const asNum = (s) => {
    if (!s) return null;
    const m = String(s).match(/([\d,]+(?:\.\d{2})?)/);
    if (!m) return null;
    return parseFloat(m[1].replace(/,/g, ""));
  };
  const inRange = (v) => typeof v === "number" && v >= 0.5 && v <= 500;

  // ---------- 트렌딩 헤더 Y 찾기 (헤더 '아래'는 전부 제외) ----------
  let trendingHeaderTop = Infinity;
  const trendHeads = Array.from(document.querySelectorAll("body *"))
    .filter(el => /what.?s trending in korea/i.test(el.textContent || ""));
  if (trendHeads.length) {
    trendHeads.sort((a,b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
    const head = trendHeads[0];
    const r = (head.closest("section,div,article") || head).getBoundingClientRect();
    trendingHeaderTop = r.top + window.scrollY;  // 헤더의 top만 쓰고 아래는 다 컷
  }

  // ---------- 베스트셀러 카드 스캔 ----------
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

    // 트렌딩 헤더 아래는 전부 제외 (Best Sellers는 페이지 상단에 위치)
    if (Number.isFinite(trendingHeaderTop) && yAbs >= (trendingHeaderTop - 10)) continue;

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = (brandEl.textContent || "").trim();

    // 상품명 (a title/aria → 명칭 셀렉터 → img alt → a 텍스트)
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

    // ---------- 가격 ----------
    // 1) price 관련 요소의 보이는 텍스트 우선
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    // 2) 없으면 카드 전체 텍스트
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const dollars = [];
    // (A) US$ 붙은 금액만 우선 채집
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) dollars.push(v);
    }
    // (B) A가 비어있을 때만, 소수 둘째자리 금액(31.50 등) 보조 채집
    if (dollars.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) dollars.push(v);
      }
    }
    // 정수 금액은 아예 사용하지 않음 (1+1, 97% 등 오인 방지)

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
      value_price_usd: valuePrice || null,
      product_url: abs,
      image_url: img || null,
    });
    seen.add(abs);
  }

  // 위→아래 정렬, 100개 제한
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  return {
    anchorCount: anchors.length,
    candidateCount: rows.length,
    picked: items.length,
    trendingHeaderTop,
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
    th = res.get("trendingHeaderTop")
    if isinstance(th, (int, float)) and math.isfinite(th):
        print(f"🧭 트렌딩 헤더 Y(top): {th:.1f}")

    items: List[Dict] = res.get("items", [])

    # 후처리
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))

    return items
