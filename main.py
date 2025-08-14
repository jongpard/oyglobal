import asyncio
import pandas as pd
from utils import kst_today_str, ensure_data_dir
from oy_global import scrape_oliveyoung_global

async def run():
    print("🔍 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return

    df = pd.DataFrame(items)
    cols = [
        "date_kst", "rank", "brand", "product_name",
        "price_current_usd", "price_original_usd", "discount_rate_pct",
        "value_price_usd", "has_value_price",
        "product_url", "image_url"
    ]
    df = df[cols]

    ensure_data_dir()
    out_path = f"data/oliveyoung_global_{kst_today_str()}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 저장 완료: {out_path}")
    print(df.head(10))

if __name__ == "__main__":
    asyncio.run(run())
