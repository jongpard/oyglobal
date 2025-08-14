# oy_global.py
import asyncio
from typing import List, Dict
from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# Top Orders 블록(1~10)만 확실하게 추출
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

  // 1) Top Orders 영역의 "1~10 번호 배지"를 이용해 카드 한 세트를 특定
  // 번호 배지는 작은 검정 사각형 안의 숫자(1~10)로 표시됨
  // -> 이 숫자 배지를 포함하는 요소들을 찾아 가장 위쪽 1~10 카드만 추출
  const rankBadgeCandidates = Array.from(document.querySelectorAll("body *")).filter(el => {
    const t = (el.textContent || "").trim();
    // 배지는 아주 짧고 순수 숫자 하나인 경우가 대부분
    return /^[1-9]$|^10$/.test(t) && isVisible(el);
  }).sort((a,b)=>absTop(a)-absTop(b));

  // 배지들 중에서 '카드 컨테이너'를 찾는다
  const CARD_SEL = "li, article, .item, .unit, .prd_info, .product, .prod, .box, .list, .list_item";
  const cardsFromBadges = [];
  for (const el of rankBadgeCandidates) {
    const card = el.closest(CARD_SEL);
    if (card && isVisible(card)) {
      cardsFromBadges.push(card);
    }
  }
  // 중복 제거
  const uniq = [];
  const seen = new Set();
  for (const c of cardsFromBadges) {
    if (!seen.has(c)) { uniq.push(c); seen.add(c); }
  }

  // 위에서부터 정렬
  uniq.sort((a,b)=>absTop(a)-absTop(b));

  // 2) 위의 카드들 중 첫 10개만 Top Orders로 사용
  const top10Cards = uniq.slice(0, 10);
  if (top10Cards.length === 0) {
    return { debug: { reason: "rank-badge based detection failed" }, items: [], candidateCount: 0, picked: 0 };
  }

  // 3) 카드 안에서 링크/브랜드/상품명/가격 파싱
  const rows = [];
  const addedUrls = new Set();

  for (let i=0; i<top10Cards.length; i++) {
    const card = top10Cards[i];
    const a = card.querySelector("a[href*='product/detail']");
    if (!a) continue;
    const href = a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (addedUrls.has(abs)) continue;

    const yAbs = absTop(card);

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

    // 가격: price 관련 요소 텍스트 우선 → 없으면 카드 전체 텍스트
    let priceText = Array.from(card.querySelectorAll(
      '[class*="price" i], [id*="price" i], [aria-label*="$" i], [aria-label*="US$" i]'
    )).map(el => (el.innerText || "").replace(/\s+/g," ")).join(" ").trim();
    if (!priceText) priceText = (card.innerText || "").replace(/\s+/g," ");

    const amounts = [];
    // (1) US$ 붙은 금액(우선)
    for (const m of priceText.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/gi)) {
      const v = asNum(m[0]); if (v != null) amounts.push(v);
    }
    // (2) 보조: US$가 전혀 없을 때만 소수 둘째자리 허용
    if (amounts.length === 0) {
      for (const m of priceText.matchAll(/\b([\d,]+\.\d{2})\b/g)) {
        const v = asNum(m[0]); if (v != null) amounts.push(v);
      }
    }
    // 정수 금액은 절대 사용하지 않음

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
    addedUrls.add(abs);
  }

  // 위→아래 정렬, 랭크 부여(1~10)
  rows.sort((a,b)=>a.y - b.y);
  const items = rows.map((r, idx) => ({ rank: idx + 1, ...r }));

  return {
    debug: { mode: "rank-badge-10", found: items.length },
    candidateCount: items.length,
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
        for _ in range(14):
            await page.mouse.wheel(0, 2400)
            await asyncio.sleep(0.5)

        res = await page.evaluate(JS_EXTRACT)
        await context.close()

    dbg = res.get("debug", {}) or {}
    print(f"🔎 Top Orders(1~10) 추출: found={dbg.get('found')}")

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))
    return items
