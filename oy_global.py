# oy_global.py
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional

from bs4 import BeautifulSoup
import pandas as pd

from price_parser import parse_price
from slack_notify import build_slack_text, post_to_slack

KST = timezone(timedelta(hours=9))
BASE_URL = "https://global.oliveyoung.com/display/page/best-seller"

def _extract_from_soup(soup: BeautifulSoup) -> List[Dict]:
    """HTML에서 베스트 상품 리스트 파싱 (정상/세일 모두 대응)."""
    out: List[Dict] = []
    items = soup.select("ul#orderBestProduct li.order-best-product.prdt-unit")
    for li in items:
        rank_el = li.select_one(".rank-badge span")
        brand_el = li.select_one("dl.brand-info > dt")
        name_el  = li.select_one("dl.brand-info > dd")

        # 가격 블록: 정가 span, 할인가 strong.point (없으면 strong)
        orig_el  = li.select_one("div.price-info span")
        sale_el  = li.select_one("div.price-info strong.point") or li.select_one("div.price-info strong")

        url_el = li.select_one("a[href*='/product/detail']")
        url = None
        if url_el:
            href = url_el.get("href", "")
            url = href if href.startswith("http") else f"https://global.oliveyoung.com{href}"

        name  = (name_el.get_text(strip=True) if name_el else "").strip()
        brand = (brand_el.get_text(strip=True) if brand_el else "").strip()

        sale_price = parse_price(sale_el.get_text()) if sale_el else None
        if orig_el:
            original_price = parse_price(orig_el.get_text())
        else:
            # 세일 표시가 없으면 strong 하나만 존재 -> 할인 없음으로 간주
            original_price = sale_price

        if not (rank_el and name and sale_price and original_price):
            continue

        rank = int(rank_el.get_text(strip=True))

        # 할인율 계산
        discount_pct = 0.0
        if original_price and original_price > 0 and sale_price is not None:
            discount_pct = round((original_price - sale_price) / original_price * 100, 2)

        out.append({
            "rank": rank,
            "brand": brand,
            "name": name,
            "original_price": float(original_price),
            "sale_price": float(sale_price),
            "discount_pct": float(discount_pct),
            "url": url,
            "raw_name": name,  # 유지
        })
    # 랭크 기준 정렬 보정
    out.sort(key=lambda x: x["rank"])
    return out

def _parse_html_str(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    return _extract_from_soup(soup)

def _load_local_html() -> Optional[str]:
    """data 폴더에 저장된 최신 page_*.html 있으면 사용 (디버그/비상용)."""
    import glob
    paths = sorted(glob.glob("data/page_*.html"))
    if not paths:
        return None
    with open(paths[-1], "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

async def _fetch_live_html() -> Optional[str]:
    """Playwright로 실시간 페이지 HTML 수집. 실패 시 None."""
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        print(f"[WARN] Playwright import 실패: {e}")
        return None

    # 한국 기준 노출을 위해 기본 로케일 ko-KR로 설정
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="ko-KR")
        page = await context.new_page()
        await page.goto(BASE_URL, timeout=60_000, wait_until="domcontentloaded")
        # 주요 리스트 로드 대기
        await page.wait_for_selector("ul#orderBestProduct li.order-best-product.prdt-unit", timeout=60_000)
        html = await page.content()
        await context.close()
        await browser.close()
        return html

def _to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    cols = ["rank", "brand", "name", "original_price", "sale_price", "discount_pct", "url", "raw_name"]
    df = pd.DataFrame(rows, columns=cols)
    return df

def _save_csv_and_json(df: pd.DataFrame) -> Tuple[str, str]:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    csv_path  = f"data/{today}_global.csv"
    json_path = f"data/{today}_global.json"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_json(json_path, orient="records", force_ascii=False)
    return csv_path, json_path

async def _scrape_impl(debug: bool = False) -> pd.DataFrame:
    html = None

    # 1) 라이브 시도
    if not debug:
        html = await _fetch_live_html()

    # 2) 실패/디버그면 로컬 덤프 사용
    if not html:
        html = _load_local_html()

    if not html:
        raise RuntimeError("페이지 HTML을 가져오지 못했습니다. (라이브/로컬 모두 실패)")

    rows = _parse_html_str(html)
    if not rows:
        raise RuntimeError("제품 리스트 파싱 실패 (rows=0)")

    df = _to_dataframe(rows)
    _save_csv_and_json(df)
    return df

def scrape_oy_global_us(debug: bool = False) -> pd.DataFrame:
    """엔트리 포인트. CSV/JSON 저장 + 슬랙 알림."""
    df_today = asyncio.run(_scrape_impl(debug=debug))

    # 전일 비교 로드 (있으면)
    prev_csv = None
    from glob import glob
    import os as _os
    files = sorted(glob("data/*_global.csv"))
    if len(files) >= 2:
        prev_csv = files[-2]

    df_prev = None
    if prev_csv and _os.path.exists(prev_csv):
        try:
            df_prev = pd.read_csv(prev_csv)
        except Exception:
            df_prev = None

    # Slack 전송
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        text = build_slack_text(datetime.now(KST), df_today.to_dict("records"), None if df_prev is None else df_prev.to_dict("records"))
        post_to_slack(webhook, text)
    else:
        print("[INFO] SLACK_WEBHOOK_URL 미설정으로 슬랙 전송 생략")

    return df_today
