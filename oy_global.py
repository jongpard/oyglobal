# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import csv
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from playwright.async_api import async_playwright, Page, Locator

BASE_URL = "https://global.oliveyoung.com/"
SEL_PRODUCT_ANCHOR = "a[href*='product/detail']"

KST = timezone(timedelta(hours=9))


def _now_kst_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _to_float(s: str) -> float:
    try:
        return float(s)
    except Exception:
        return 0.0


def _split_brand_and_product(anchor_text: str) -> (str, str):
    lines = [ln.strip() for ln in anchor_text.splitlines() if ln.strip()]
    if not lines:
        return "", ""

    brand = lines[0]
    rest = " ".join(lines[1:]).strip()

    if not rest:
        txt = " ".join(lines)
        if txt.lower().startswith((brand + " ").lower()):
            rest = txt[len(brand):].strip()
        else:
            rest = txt

    pattern = re.compile(rf"^{re.escape(brand)}\s+", re.IGNORECASE)
    product = pattern.sub("", rest).strip()
    return brand, product


_price_cur_re = re.compile(r"US\$\s*([0-9]+(?:\.[0-9]+)?)")
_price_val_re = re.compile(r"(?:정가|Value)\s*[: ]?\s*US\$\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)


def _parse_prices(text: str) -> Dict[str, float | int]:
    cur = 0.0
    val = 0.0

    m_cur = _price_cur_re.search(text)
    if m_cur:
        cur = _to_float(m_cur.group(1))

    m_val = _price_val_re.search(text)
    if m_val:
        val = _to_float(m_val.group(1))

    disc_int = 0
    if val > 0 and cur > 0 and val >= cur:
        disc_int = int(round((val - cur) / val * 100))  # ← 정수 % 로 반올림

    return dict(
        price_current_usd=cur,
        price_original_usd=val,
        discount_rate_pct=disc_int,  # ← 정수
        value_price_usd=val,
        has_value_price=1 if val > 0 else 0,
    )


async def _get_section_locator(page: Page, title_regex: re.Pattern) -> Locator:
    heading = page.locator("h2, h3").filter(has_text=title_regex).first
    await heading.wait_for(state="visible", timeout=30000)
    section = heading.locator("xpath=ancestor::*[self::section or self::div][1]")
    return section


async def _autoscroll_collect(section: Locator, need: int = 100) -> List[Locator]:
    seen = set()
    anchors: List[Locator] = []
    await section.scroll_into_view_if_needed()

    idle_rounds = 0
    while True:
        await asyncio.sleep(0.3)
        new_found = 0

        batch = section.locator(SEL_PRODUCT_ANCHOR)
        count = await batch.count()
        for i in range(count):
            a = batch.nth(i)
            href = await a.get_attribute("href") or ""
            if not href or href in seen:
                continue

            card = a.locator("xpath=ancestor::*[self::li or self::div][1]")
            try:
                await card.scroll_into_view_if_needed()
            except Exception:
                pass

            anchors.append(a)
            seen.add(href)
            new_found += 1

        if len(anchors) >= need:
            break

        if new_found == 0:
            idle_rounds += 1
        else:
            idle_rounds = 0

        if idle_rounds >= 5:
            break

        try:
            await section.evaluate("(el) => el.scrollBy(0, el.clientHeight)")
        except Exception:
            break

    return anchors[:need]


async def _extract_item_from_anchor(a: Locator) -> Dict:
    href = await a.get_attribute("href") or ""
    product_url = href if href.startswith("http") else (BASE_URL.rstrip("/") + "/" + href.lstrip("/"))

    card = a.locator("xpath=ancestor::*[self::li or self::div][1]")
    card_text = await card.inner_text()

    anchor_text = await a.inner_text()
    brand, product = _split_brand_and_product(anchor_text)

    img = ""
    try:
        img = await a.locator("img").first.get_attribute("src") or ""
    except Exception:
        pass

    prices = _parse_prices(card_text)

    return dict(
        brand=brand,
        product_name=product or brand,
        price_current_usd=prices["price_current_usd"],
        price_original_usd=prices["price_original_usd"],
        discount_rate_pct=prices["discount_rate_pct"],  # 정수
        value_price_usd=prices["value_price_usd"],
        has_value_price=prices["has_value_price"],
        product_url=product_url,
        image_url=img,
    )


async def scrape_oliveyoung_global() -> List[Dict]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1366, "height": 900})
        await page.goto(BASE_URL, wait_until="domcontentloaded")

        # Top Orders / Best Sellers / TOP10/50/100 등 폭넓게 매칭
        title_re = re.compile(r"(Top\s*Orders|Best\s*Sellers|TOP\s*100|TOP\s*50|TOP\s*10)", re.I)
        section = await _get_section_locator(page, title_re)

        await section.locator(SEL_PRODUCT_ANCHOR).first.wait_for(state="visible", timeout=30000)
        anchors = await _autoscroll_collect(section, need=100)

        items: List[Dict] = []
        for a in anchors:
            try:
                items.append(await _extract_item_from_anchor(a))
            except Exception:
                continue

        await browser.close()

    date_kst = _now_kst_date()
    for i, it in enumerate(items, start=1):
        it["date_kst"] = date_kst
        it["rank"] = i

    return items


# ----------------- CSV -----------------
CSV_HEADER = [
    "date_kst",
    "rank",
    "brand",
    "product_name",
    "price_current_usd",
    "price_original_usd",
    "discount_rate_pct",
    "value_price_usd",
    "has_value_price",
    "product_url",
    "image_url",
]


def save_csv(items: List[Dict], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writeheader()
        for it in items:
            row = {k: it.get(k, "") for k in CSV_HEADER}
            w.writerow(row)


if __name__ == "__main__":
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    out = f"data/oliveyoung_global_{_now_kst_date()}.csv"
    items = asyncio.run(scrape_oliveyoung_global())
    save_csv(items, out)
    print(f"📁 저장 완료: {out}")
