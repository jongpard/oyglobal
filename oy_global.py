# oy_global.py
import asyncio
import math
from typing import List, Dict
from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

# 페이지 안에서 "Top Orders(베스트셀러)" 섹션만 정확히 추출
JS_EXTRACT = r"""
() => {
  const asNum = (s) => {
    if (!s) return null;
    const m = String(s).match(/([\d,]+(?:\.\d{2})?)/);
    if (!m) return null;
    return parseFloat(m[1].replace(/,/g, ""));
  };
  const inRange = (v) => typeof v === "number" && v >= 0.5 && v <= 500;

  // ---------- 1) Top Orders 섹션 컨테이너 찾기 ----------
  const FILTER_WORDS = [
    "All","Skincare","Makeup","Bath & Body","Hair","Face Masks","Suncare",
    "Makeup Brush & Tools","Wellness","Supplements","Food & Drink"
  ].map(s => s.toLowerCase());

  let topOrdersContainer = null;

  // 후보: 필터 칩 텍스트를 모두(혹은 대부분) 포함하는 노드
  const nodes = Array.from(document.querySelectorAll("body *")).filter(el => {
    const t = (el.textContent || "").toLowerCase();
    let hit = 0;
    for (const w of FILTER_WORDS) if (t.includes(w)) hit++;
    return hit >= 5; // 5개 이상 포함하면 필터 바로 간주
  });

  // 가장 위에 있는 필터 바를 기준으로, 적당한 조상 컨테이너 선택
  if (nodes.length) {
    nodes.sort((a,b)=> a.getBoundingClientRect().top - b.getBoundingClientRect().top);
    const bar = nodes[0];

    // 위로 올라가며 '상품 상세 링크'를 적절히 포함하는 조상 컨테이너 선택
    const MIN_LINKS = 20;  // 최소 링크 수
    const MAX_LINKS = 160; // 너무 크면 페이지 전체일 수 있으므로 상한
    let cur = bar;
    for (let i=0; i<10 && cur; i++, cur = cur.parentElement) {
      const cnt = cur.querySelectorAll("a[href*='product/detail']").length;
      if (cnt >= MIN_LINKS && cnt <= MAX_LINKS) {
        topOrdersContainer = cur;
        break;
      }
    }
    // 실패하면 바로 상위 섹션/디브라도 사용 (최후보)
    if (!topOrdersContainer) topOrdersContainer = bar.closest("section,div,article") || bar.parentElement || bar;
  }

  // ---------- 2) 섹션 안의 카드만 수집 ----------
  const allAnchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const anchors = topOrdersContainer
      ? Array.from(topOrdersContainer.querySelectorAll("a[href*='product/detail']"))
      : allAnchors; // 안전망: 못 찾으면 전체(후에 정렬로 Top Orders가 먼저 나옴)

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
    if (!name || name.length < 3) name = a.textContent || "";
    name = (name || "").replace(/\s+/g, " ").trim();
    if (!name) name = "상품";

    // 이미지
    let img = "";
    const imgEl = card.querySelector("img");
    if (imgEl) img = imgEl.src || imgEl.getAttribute("src") || "";

    // ---------- 가격 ----------
    // 1) price 관련 요소 텍스트 우선
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
    // (B) A가 비었을 때만 소수 둘째자리 보조
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

  // ---------- 3) 위→아래 정렬 후 상위 100개 ----------
  rows.sort((a, b) => a.y - b.y);
  const items = rows.slice(0, 100).map((r, i) => ({ rank: i + 1, ...r }));

  // 디버그 정보
  let info = { anchors_total: document.querySelectorAll("a[href*='product/detail']").length };
  if (topOrdersContainer) {
    const r = topOrdersContainer.getBoundingClientRect();
    info.top_orders = {
      links: topOrdersContainer.querySelectorAll("a[href*='product/detail']").length,
      y_top: r.top + window.scrollY,
      y_bottom: r.bottom + window.scrollY
    };
  }

  return { info, candidateCount: rows.length, picked: items.length, items };
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

    info = res.get("info", {}) or {}
    print(f"🔎 전체 앵커: {info.get('anchors_total')}, 후보 카드: {res.get('candidateCount')}, 최종 채택: {res.get('picked')}")
    if "top_orders" in info and isinstance(info["top_orders"], dict):
        to = info["top_orders"]
        print(f"🧭 Top Orders 섹션: links={to.get('links')}, y=({to.get('y_top'):.1f}~{to.get('y_bottom'):.1f})")

    items: List[Dict] = res.get("items", [])
    for r in items:
        r["date_kst"] = kst_today_str()
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = bool(r.get("value_price_usd"))

    return items
