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
  const scrollTop = () => window.pageYOffset || document.documentElement.scrollTop || document.body.scrollTop || 0;
  const absTop = (el) => (el.getBoundingClientRect().top + scrollTop());
  const isVisible = (el) => {
    if (!el) return false;
    const st = getComputedStyle(el);
    if (st.visibility === "hidden" || st.display === "none") return false;
    const r = el.getBoundingClientRect();
    return r.width > 0 && r.height > 0;
  };

  // --------- 트렌딩 섹션 루트 찾기(가시 요소만) ----------
  const headAll = Array.from(document.querySelectorAll("body *"))
    .filter(el => /what.?s\s+trending\s+in\s+korea/i.test((el.textContent || "").trim()))
    .filter(isVisible)
    .sort((a,b)=>absTop(a)-absTop(b));

  let trendingRoot = null;
  if (headAll.length) {
    let cur = headAll[0];
    // 헤더에서 위로 올라가며 "적정 수의 상품 링크"를 품은 조상 선택
    for (let i=0; i<12 && cur; i++, cur = cur.parentElement) {
      const cnt = cur.querySelectorAll("a[href*='product/detail']").length;
      if (cnt >= 6 && cnt <= 80) { trendingRoot = cur; break; }
    }
    if (trendingRoot && trendingRoot.querySelectorAll("a[href*='product/detail']").length === 0) {
      trendingRoot = null;
    }
  }

  // --------- 랭크 뱃지(1~100) 기반으로 카드 수집 ----------
  const CARD_SEL = "li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item";
  const badgeEls = Array.from(document.querySelectorAll("body *"))
    .filter(el => {
      const t = (el.textContent || "").trim();
      return /^[1-9]\d?$|^100$/.test(t) && t.length <= 3 && isVisible(el);
    })
    .sort((a,b)=>absTop(a)-absTop(b));

  // 배지 → 카드, 랭크
  const rankCards = [];
  const seenCards = new Set();
  for (const b of badgeEls) {
    const rank = parseInt((b.textContent || "").trim(), 10);
    if (!(rank >=1 && rank <= 100)) continue;

    const card = b.closest(CARD_SEL);
    if (!card || !isVisible(card)) continue;
    // 상품 링크가 없으면 스킵
    const a = card.querySelector("a[href*='product/detail']");
    if (!a) continue;

    // 트렌딩 섹션 내부는 제외
    if (trendingRoot && trendingRoot.contains(card)) continue;

    if (!seenCards.has(card)) {
      rankCards.push({ rank, card, a });
      seenCards.add(card);
    }
  }

  // 랭크 중복 제거(동일 랭크가 여러 카드로 잡혔을 때 가장 위쪽 하나만 유지)
  const byRank = new Map();
  for (const rc of rankCards) {
    if (!byRank.has(rc.rank)) byRank.set(rc.rank, rc);
  }

  // 랭크 1..100 정렬
  const ordered = Array.from(byRank.values()).sort((x,y)=>x.rank - y.rank);

  // 카드 → 데이터 파싱
  const rows = [];
  const added = new Set();

  for (const {rank, card, a} of ordered) {
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (added.has(abs)) continue;

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

    // 가격 텍스트
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const amounts = [];
    // US$ 우선
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) amounts.push(v);
    }
    // 보조: US$가 전혀 없을 때만 소수 둘째자리 허용
    if (amounts.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) amounts.push(v);
      }
    }

    // Value: US$xx.xx → 정가
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
      rank,
      brand: brand || null,
      product_name: name || "상품",
      price_current_usd: priceCur,
      price_original_usd: priceOri,
      value_price_usd: valuePrice || null,
      product_url: abs,
      image_url: img || null,
    });
    added.add(abs);
  }

  const items = rows
    .sort((a,b)=>a.rank - b.rank)
    .slice(0, 100);

  return {
    debug: {
      trending_found: !!trendingRoot,
      badges_total: badgeEls.length,
      rank_candidates: rankCards.length,
      distinct_ranks: byRank.size,
      items_out: items.length
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

        # 더 아래까지 충분히 로드 (Top 100 끝까지)
        prev = -1
        same = 0
        for i in range(60):
            await page.mouse.wheel(0, 3200)
            await asyncio.sleep(0.6)
            cnt = await page.locator("a[href*='product/detail']").count()
            if cnt == prev: same += 1
            else: same = 0
            prev = cnt
            if i >= 18 and same >= 3:
                break

        res = await page.evaluate(JS_EXTRACT)
        await context.close()

    dbg = res.get("debug", {}) or {}
    print(f"🔎 트렌딩 찾음={dbg.get('trending_found')}, 뱃지={dbg.get('badges_total')}, 후보={dbg.get('rank_candidates')}, 고유랭크={dbg.get('distinct_ranks')}, 최종={dbg.get('items_out')}")

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))
    return items
