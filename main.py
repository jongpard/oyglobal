# main.py
import os
import asyncio
import pandas as pd
from oy_global import scrape_oliveyoung_global
from utils import kst_today_str

CSV_DIR = "data"

async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()

    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return

    df = pd.DataFrame(items)

    # CSV 스키마(필수 컬럼) – 누락 시 자동 생성
    cols = [
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
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].sort_values("rank")

    os.makedirs(CSV_DIR, exist_ok=True)
    fname = os.path.join(CSV_DIR, f"oliveyoung_global_{kst_today_str()}.csv")
    df.to_csv(fname, index=False, encoding="utf-8")
    print(f"💾 저장 완료: {fname}")
    print(df.head(10)[["date_kst","rank","product_name","price_current_usd","price_original_usd"]])

if __name__ == "__main__":
    asyncio.run(run())
