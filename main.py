# -*- coding: utf-8 -*-
import os
import asyncio
from datetime import datetime, timezone, timedelta
import pandas as pd

from oy_global import scrape_oliveyoung_global
from slack_top10 import post_top10_to_slack

KST = timezone(timedelta(hours=9))

CSV_DIR = "data"
CSV_NAME_TMPL = "oliveyoung_global_{date}.csv"
CSV_COLUMNS = [
    "date_kst", "rank", "brand", "product_name",
    "price_current_usd", "price_original_usd", "discount_rate_pct",
    "value_price_usd", "has_value_price",
    "product_url", "image_url"
]

def ensure_dirs():
    os.makedirs(CSV_DIR, exist_ok=True)

def now_kst_date():
    return datetime.now(KST).strftime("%Y-%m-%d")

async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    ensure_dirs()

    items = await scrape_oliveyoung_global()  # List[dict], 1~100위
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return

    # DataFrame 생성 + 컬럼 고정 순서
    for it in items:
        # 누락키 보정
        for k in CSV_COLUMNS:
            it.setdefault(k, None)

    df = pd.DataFrame(items)[CSV_COLUMNS]

    # 저장
    out_path = os.path.join(CSV_DIR, CSV_NAME_TMPL.format(date=now_kst_date()))
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"📁 저장 완료: {out_path}")

    # 상위 10 슬랙 전송(웹훅이 없으면 조용히 패스)
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if webhook:
        ok = post_top10_to_slack(webhook, df.head(10))
        print("Sent Slack message. status=", ok)
    else:
        print("ℹ️ SLACK_WEBHOOK_URL 미설정: 슬랙 전송 생략")

if __name__ == "__main__":
    asyncio.run(run())
