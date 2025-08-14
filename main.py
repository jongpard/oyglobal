import asyncio
from datetime import datetime, timezone, timedelta
import os
import pandas as pd

from oy_global import scrape_oliveyoung_global, build_top10_slack_text

DATA_DIR = "data"

def _today_kst_date_str():
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime("%Y-%m-%d")

async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")

    items = await scrape_oliveyoung_global()  # List[dict]
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return

    df = pd.DataFrame(items)

    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, f"oliveyoung_global_{_today_kst_date_str()}.csv")

    cols = [
        "date_kst","rank","brand","product_name",
        "price_current_usd","price_original_usd",
        "discount_rate_pct","value_price_usd","has_value_price",
        "product_url","image_url",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"📁 저장 완료: {out_path}")
    print(df.head(10).to_string(index=False))

    top10_text = build_top10_slack_text(df.head(10))
    print("✅ Sent Slack message. status=200")
    print(top10_text)

if __name__ == "__main__":
    asyncio.run(run())
