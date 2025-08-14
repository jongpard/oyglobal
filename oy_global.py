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
  const scrollTop = () => window.pageYOffset || document.documentElement.scrollTop || document.body.scrollTop || 0;
  const absTop = (el) => (el.getBoundingClientRect().top + scrollTop());
  const isVisible = (el) => {
    if (!el) return false;
    const style = getComputedStyle(el);
    if (style.visibility === "hidden" || style.display === "none") return false;
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0;
  };

  // ---------- A) Top Orders 필터 바로 섹션 컨테이너 ----------
  const FILTER_WORDS = [
    "All","Skincare","Makeup","Bath & Body","Hair","Face Masks","Suncare",
    "Makeup Brush & Tools","Wellness","Supplements","Food & Drink"
  ].map(s => s.toLowerCase());

  let topOrdersContainer = null;

  const filterBars = Array.from(document.querySelectorAll("body *")).filter(el => {
    const t = (el.textContent || "").toLowerCase();
    let hit = 0; for (const w of FILTER_WORDS) if (t.includes(w)) hit++;
    return hit >= 5;
  }).sort((a,b)=>absTop(a)-absTop(b));

  if (filterBars.length) {
    const bar = filterBars[0];
    const MIN_LINKS = 20, MAX_LINKS = 200;
    let cur = bar;
    for (let i=0; i<12 && cur; i++, cur = cur.parentElement) {
      const cnt = cur.querySelectorAll("a[href*='product/detail']").length;
      if (cnt >= MIN_LINKS && cnt <= MAX_LINKS) { topOrdersContainer = cur; break; }
    }
    if (!topOrdersContainer) topOrdersContainer = bar.closest("section,div,article") || bar.parentElement || bar;
  }

  // ---------- B) '보이는' 트렌딩 헤더의 절대 Y ----------
  const trendHeadsAll = Array.from(document.querySelectorAll("body *")).filter(el =>
    /what.?s\s+trending\s+in\s+korea/i.test((el.textContent || "").trim())
  );
  const trendHeadsVisible = trendHeadsAll
    .filter(isVisible)
    .map(el => ({el, y: absTop(el)}))
    // 트렌딩은 페이지 중단 이후에 위치하므로 너무 상단(노이즈) 배제
    .filter(x => x.y > 400)
    .sort((a,b)=>a.y - b.y);

  const trendingY = trendHeadsVisible.length ? trendHeadsVisible[0].y : Infinity;

  // ---------- C) Top Orders 섹션 내 카드만, 그리고 y < trendingY ----------
  const anchorsAll = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const anchors = topOrdersContainer
    ? Array.from(topOrdersContainer.querySelectorAll("a[href*='product/detail']"))
    : anchorsAll;

  const seen = new Set();
  const rows = [];

  for (const a of anchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest("li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item") || a;
    const yAbs = absTop(card);

    if (yAbs >= trendingY) continue;  // 트렌딩 헤더 아래는 컷

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
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const amounts = [];
    // 1) US$ 붙은 금액
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) amounts.push(v);
    }
    // 2) 보조: US$가 전혀 없을 때만 소수 둘째자리 허용
    if (amounts.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) amounts.push(v);
      }
    }
    // 정수는 절대 사용하지 않음

    // Value: US$xx → 정가 힌트
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

  // ---------- 정렬 후 상위 100 ----------
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  return {
    debug: {
      filter_bars: filterBars.length,
      top_orders_links: topOrdersContainer ? topOrdersContainer.querySelectorAll("a[href*='product/detail']").length : 0,
      trendingY: Number.isFinite(trendingY) ? trendingY : null,
      anchors_considered: anchors.length,
    },
    candidateCount: rows.length,
    picked: items.length,
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
        prev = -1; same = 0
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

    dbg = res.get("debug", {}) or {}
    print(f"🔎 섹션 앵커수={dbg.get('anchors_considered')}, 후보={res.get('candidateCount')}, 최종={res.get('picked')}")
    print(f"🧭 필터바={dbg.get('filter_bars')}, TopOrders링크={dbg.get('top_orders_links')}, 트렌딩Y={dbg.get('trendingY')}")

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))

    return items
