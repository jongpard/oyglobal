# oy_global.py
import asyncio
from typing import List, Dict
from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# Top Orders 그리드만 정확히 긁기: 첫 10개 카드의 LCA(최저 공통 조상) 컨테이너만 사용
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

  // 카드(상품) 엘리먼트 찾기
  const CARD_SEL = "li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item";
  const allAnchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));

  // 앵커 -> 카드 컨테이너, y좌표
  const cards = [];
  for (const a of allAnchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const card = a.closest(CARD_SEL) || a;
    const y = absTop(card);
    cards.push({ a, card, y, href });
  }
  if (cards.length === 0) {
    return { debug: { anchors_total: allAnchors.length, lca_found: false }, items: [], candidateCount: 0, picked: 0 };
  }

  // y 오름차순 정렬 후 "첫 10개 카드"의 LCA(최저 공통 조상) 찾기
  cards.sort((x,y)=>x.y - y.y);
  const seedCards = cards.slice(0, Math.min(10, cards.length)).map(x => x.card);

  const findLCA = (els) => {
    if (!els.length) return null;
    // 기준: 첫 카드에서 위로 올라가며 모든 카드를 포함하는 가장 낮은 조상
    for (let n = els[0]; n; n = n.parentElement) {
      let ok = true;
      for (let i = 1; i < els.length; i++) {
        if (!n.contains(els[i])) { ok = false; break; }
      }
      if (ok) return n;
    }
    return document.body;
  };

  const lca = findLCA(seedCards);
  if (!lca) {
    return { debug: { anchors_total: allAnchors.length, lca_found: false }, items: [], candidateCount: 0, picked: 0 };
  }

  // LCA 컨테이너 내부의 상품만 대상으로 삼음 (= Top Orders 그리드 영역)
  const anchors = Array.from(lca.querySelectorAll("a[href*='product/detail']"));
  const seen = new Set();
  const rows = [];

  for (const a of anchors) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest(CARD_SEL) || a;
    const yAbs = absTop(card);

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = (brandEl.textContent || "").trim();

    // 상품명: a title/aria → 명칭 엘리먼트 → img alt → a 텍스트
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

    // 가격 텍스트: price 관련 요소 → 없으면 카드 전체 텍스트
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    // 금액 파싱
    const amounts = [];
    // 1) US$ 붙은 금액(우선)
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) amounts.push(v);
    }
    // 2) 보조: US$가 전혀 없을 때만 소수 둘째자리 허용
    if (amounts.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) amounts.push(v);
      }
    }
    // 정수 금액은 절대 사용하지 않음

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

  // 위→아래 정렬, 상위 100개만
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  // 디버그
  const l = lca.getBoundingClientRect();
  return {
    debug: {
      anchors_total: allAnchors.length,
      lca_found: true,
      lca_child_anchors: anchors.length,
      lca_rect_top_abs: l.top + scrollTop(),
      first10_y_min: Math.min(...seedCards.map(el => absTop(el))),
      first10_y_max: Math.max(...seedCards.map(el => absTop(el))),
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

    dbg = res.get("debug", {}) or {}
    print(
        f"🔎 LCA내 앵커={dbg.get('lca_child_anchors')}, 후보={res.get('candidateCount')}, 최종={res.get('picked')}"
        f" | 전체앵커={dbg.get('anchors_total')}, LCA top={dbg.get('lca_rect_top_abs')}"
        f" | first10 y=({dbg.get('first10_y_min')}~{dbg.get('first10_y_max')})"
    )

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))

    return items
