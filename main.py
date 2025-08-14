# -*- coding: utf-8 -*-
import asyncio
import os
from datetime import datetime, timezone, timedelta

import pandas as pd

from oy_global import scrape_oliveyoung_global
from slack_notify import post_top10_to_slack

KST = timezone(timedelta(hours=9))


async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()  # List[dict], 1~100위

    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return

    # DataFrame & 저장
    df = pd.DataFrame(items)
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")
    out_path = f"data/oliveyoung_global_{today_kst}.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"📁 저장 완료: {out_path}")

    # 상위 10개 슬랙 알림 (환경변수 없으면 생략)
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if webhook:
        post_top10_to_slack(out_path, webhook_url=webhook)
        print("✅ Slack message sent.")
    else:
        print("ℹ️ SLACK_WEBHOOK_URL 없음: 슬랙 전송 생략.")


if __name__ == "__main__":
    asyncio.run(run())
