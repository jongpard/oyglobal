import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pandas as pd
from playwright.async_api import async_playwright, Page


KST = timezone(timedelta(hours=9))


def _today_kst_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _usd_to_float(s: str) -> float:
    # 'US$54.00', 'US$1,234.56', 'US$ 25.99' 등 처리
    m = re.search(r"US\$?\s*([0-9][0-9,]*\.?[0-9]*)", s)
    if not m:
        return 0.0
    return float(m.group(1).replace(",", ""))


async def _autoscroll_collect(page: Page, need: int = 100, pause_ms: int = 350) -> List[Dict]:
    """
    홈의 Top Orders 그리드에서 product/detail 링크를 DOM 순서대로 수집.
    - '트렌딩' 섹션을 구분하지 않고, **100개 모이면 즉시 중단** → 트렌딩 포함 방지.
    - 카드 컨테이너에서 브랜드/상품/가격을 파싱.
    """
    # ✅ 가시성(visible) 대신 DOM 부착(attached)만 보장해서 첫 요소가 안 보이는 경우 타임아웃 방지
    await page.wait_for_selector("a[href*='product/detail']", state="attached", timeout=30000)

    seen_hrefs = set()
    items: List[Dict] = []
    idle_rounds = 0

    async def parse_one(anchor) -> Dict:
        href = await anchor.get_attribute("href")
        if not href:
            return {}
        href = href.strip()

        # 카드 컨테이너(상위 li/div) 추정
        container = anchor.locator("xpath=ancestor::li[1]")
        if not await container.count():
            container = anchor.locator("xpath=ancestor::div[1]")

        # 이미지
        img = container.locator("img")
        image_url = ""
        try:
            if await img.count():
                image_url = (await img.first.get_attribute("src")) or ""
        except:
            pass

        # 상품명은 anchor 텍스트
        product_name = (await anchor.inner_text()).strip()

        # 브랜드: 카드 내의 a 중 product/detail이 **아닌** 것들 중 첫 a 텍스트를 사용
        brand = ""
        try:
            brand_links = container.locator("a:not([href*='product/detail'])")
            if await brand_links.count():
                for i in range(await brand_links.count()):
                    t = (await brand_links.nth(i).inner_text()).strip()
                    # 너무 긴 문장/가격/잡텍스트 제거
                    if t and not re.search(r"US\$", t) and len(t) <= 40:
                        brand = t
                        break
        except:
            pass

        # 가격 파싱: 카드 텍스트에서 전부 뽑아 정리
        text = ""
        try:
            text = (await container.inner_text()).replace("\n", " ")
        except:
            pass

        # Value 가격(정가)이 따로 표시되는 경우
        value_price = 0.0
        m_val = re.search(r"Value\s*:\s*US\$?\s*([0-9][0-9,]*\.?[0-9]*)", text, re.I)
        if m_val:
            value_price = float(m_val.group(1).replace(",", ""))

        # 카드 내 모든 US$ 금액
        nums = [float(x.replace(",", "")) for x in re.findall(r"US\$?\s*([0-9][0-9,]*\.?[0-9]*)", text)]

        price_current = 0.0
        price_original = 0.0

        if value_price > 0 and len(nums) >= 1:
            price_current = nums[0]
            price_original = value_price
        elif len(nums) >= 2:
            # 보편적으로 [정가, 현재가] 순서
            price_original, price_current = nums[0], nums[1]
        elif len(nums) == 1:
            price_current = nums[0]
            price_original = price_current
        else:
            return {}

        # discount
        discount_pct = 0.0
        if price_original > 0:
            discount_pct = round((1 - (price_current / price_original)) * 100, 2)

        return {
            "brand": brand,
            "product_name": product_name,
            "price_current_usd": price_current,
            "price_original_usd": price_original,
            "discount_rate_pct": discount_pct,
            "value_price_usd": value_price if value_price > 0 else "",
            "has_value_price": bool(value_price > 0),
            "product_url": href,
            "image_url": image_url,
        }

    # 스크롤하며 수집
    while len(items) < need:
        anchors = page.locator("a[href*='product/detail']:visible")
        count = await anchors.count()
        new_in_this_round = 0

        for i in range(count):
            a = anchors.nth(i)
            href = await a.get_attribute("href")
            if not href:
                continue
            href = href.strip()
            if href in seen_hrefs:
                continue

            parsed = await parse_one(a)
            if not parsed:
                continue

            seen_hrefs.add(href)
            items.append(parsed)
            new_in_this_round += 1

            if len(items) >= need:
                break

        if len(items) >= need:
            break

        # 새로 얻은 게 없으면 몇 번만 더 스크롤 시도
        if new_in_this_round == 0:
            idle_rounds += 1
        else:
            idle_rounds = 0

        if idle_rounds >= 5:
            break

        # 아래로 스크롤
        await page.mouse.wheel(0, 2000)
        await page.wait_for_timeout(pause_ms)

    return items[:need]


async def scrape_oliveyoung_global() -> List[Dict]:
    """
    올리브영 **글로벌몰 홈 Top Orders** 1~100위만 수집.
    - 트렌딩 섹션은 무시(100개 수집되면 즉시 중단).
    - CSV용 dict 리스트 반환.
    """
    url = "https://global.oliveyoung.com/"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(1000)

        data = await _autoscroll_collect(page, need=100)

        # 최종 정돈 + rank, date_kst 부여
        for idx, row in enumerate(data, start=1):
            brand = (row.get("brand") or "").strip()
            if len(brand) > 40:
                brand = ""
            row["brand"] = brand

            row["rank"] = idx
            row["date_kst"] = _today_kst_str()

        await browser.close()
        return data


def save_to_csv(rows: List[Dict]) -> str:
    if not rows:
        raise RuntimeError("No rows to save.")
    df = pd.DataFrame(rows, columns=[
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
    ])
    out = f"data/oliveyoung_global_{_today_kst_str()}.csv"
    df.to_csv(out, index=False)
    return out


if __name__ == "__main__":
    async def _run():
        rows = await scrape_oliveyoung_global()
        path = save_to_csv(rows)
        print("Saved:", path)
    asyncio.run(_run())
