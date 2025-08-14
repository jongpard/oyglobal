# oy_global.py
import asyncio
from typing import List, Dict

from playwright.async_api import async_playwright
from utils import kst_today_str

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"

JS_EXTRACT = r"""
() => {
  // 헬퍼
  const asNum = (s) => {
    if (!s) return null;
    const m = String(s).match(/([\d,]+(?:\.\d{2})?)/);
    if (!m) return null;
    return parseFloat(m[1].replace(/,/g, ""));
  };

  // 트렌딩 섹션 y-range 찾기
  let trendTop = -Infinity, trendBottom = -Infinity;
  const all = Array.from(document.querySelectorAll("body *"));
  const trendNode = all.find(el => /what.?s trending in korea/i.test(el.textContent || ""));
  if (trendNode) {
    const box = (trendNode.closest("section,div,article") || trendNode).getBoundingClientRect();
    trendTop = box.top + window.scrollY;
    trendBottom = box.bottom + window.scrollY;
  }

  // 베스트 카드 = 상품 상세로 가는 링크가 있는 가장 가까운 카드
  const anchors = Array.from(document.querySelectorAll("a[href*='product/detail']"));
  const seen = new Set();
  const rows = [];

  for (const a of anchors) {
    const href = a.href || a.getAttribute("href") || "";
    if (!href) continue;
    const abs = href.startsWith("http") ? href : (location.origin + href);
    if (seen.has(abs)) continue;

    const card = a.closest("li, article, .item, .unit, .prd_info, .product, .prod, .box") || a;
    const rect = card.getBoundingClientRect();
    const yAbs = rect.top + window.scrollY;

    // 트렌딩 구간 제외
    if (trendTop > -Infinity && yAbs >= trendTop && yAbs <= trendBottom) continue;

    // 브랜드
    let brand = "";
    const brandEl = card.querySelector('[class*="brand" i], strong.brand');
    if (brandEl) brand = brandEl.textContent.trim();

    // 상품명: a의 title/aria-label/텍스트 순
    let name = a.getAttribute("title") || a.getAttribute("aria-label") || a.textContent || "";
    name = name.replace(/\s+/g, " ").trim();
    if (!name || name.length < 3) {
      const nameEl = card.querySelector("p.name, .name, .prd_name, .product-name, strong.name");
      if (nameEl) name = (nameEl.textContent || "").replace(/\s+/g, " ").trim();
    }

    // 이미지
    let img = "";
    const imgEl = card.querySelector("img");
    if (imgEl) img = imgEl.src || imgEl.getAttribute("src") || "";

    // 가격: 카드의 '보이는 텍스트'만 사용
    const visible = (card.innerText || "").replace(/\s+/g, " ");
    const allDollar = Array.from(visible.matchAll(/US\$ ?([\d,]+(?:\.\d{2})?)/g)).map(m => asNum(m[0]));
    // Value: US$xx.xx → 정가로 취급
    const vm = visible.match(/value\s*[:：]?\s*US\$ ?([\d,]+(?:\.\d{2})?)/i);
    const valuePrice = vm ? asNum(vm[0]) : null;

    let priceCur = null, priceOri = null;
    if (allDollar.length >= 1) {
      // 일반: 가장 작은 값 = 현재가
      priceCur = Math.min(...allDollar);
      if (valuePrice != null) {
        priceOri = valuePrice;
      } else if (allDollar.length >= 2) {
        priceOri = Math.max(...allDollar);
      } else {
        priceOri = priceCur;
      }
    }

    // 금액 sanity 체크(0.5~500달러만)
    const ok = (v) => typeof v === "number" && v >= 0.5 && v <= 500;
    if (!ok(priceCur)) continue;
    if (!ok(priceOri)) priceOri = priceCur;

    seen.add(abs);
    rows.push({
      y: yAbs,
      brand: brand || null,
      product_name: name || "상품",
      price_current_usd: priceCur,
      price_original_usd: priceOri,
      product_url: abs,
      image_url: img || null,
    });
  }

  // y(위->아래) 정렬, 100개 제한
  rows.sort((a, b) => a.y - b.y);
  const out = rows.slice(0, 100).map((r, idx) => ({
    rank: idx + 1,
    ...r
  }));
  return out;
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

        # 끝까지 스크롤(보이는대로 수집)
        prev = -1
        same = 0
        for i in range(35):
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(0.6)
            cnt = await page.locator("a[href*='product/detail']").count()
            if cnt == prev:
                same += 1
            else:
                same = 0
            prev = cnt
            if i > 12 and same >= 3:
                break

        # 페이지 안에서 한 번에 추출
        rows = await page.evaluate(JS_EXTRACT)

        await context.close()

    # 파이썬 측에서 마무리(날짜 붙이고 필드 순서 정리)
    for r in rows:
        r["date_kst"] = kst_today_str()
        # 할인율
        cur, ori = r["price_current_usd"], r["price_original_usd"]
        r["discount_rate_pct"] = round((1 - cur / ori) * 100, 2) if ori and ori > 0 else 0.0
        r["has_value_price"] = False  # (글로벌몰은 Value 문구만 정가 힌트로 쓰고 별도 저장 X)

    # CSV 저장/리턴은 main.py가 처리
    return rows
