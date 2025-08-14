# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import csv
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from oy_global import scrape_oliveyoung_global
from slack_notify import post_top10_to_slack

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

KST = timezone(timedelta(hours=9))


def _today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _csv_path() -> str:
    return os.path.join(DATA_DIR, f"oliveyoung_global_{_today_kst()}.csv")


def _save_csv(rows: List[Dict], path: str) -> None:
    if not rows:
        print("⚠️ 수집 결과가 비어있습니다.")
        return
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
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"📁 저장 완료: {path}")


async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()
    # 1~100위가 아니면 정렬/재랭크 보정
    items = sorted(items, key=lambda x: x.get("rank", 10**9))
    for i, it in enumerate(items, 1):
        it["rank"] = i

    csv_path = _csv_path()
    _save_csv(items, csv_path)

    # 슬랙 전송(환경변수 있을 때만)
    if os.getenv("SLACK_WEBHOOK_URL"):
        post_top10_to_slack(csv_path)


if __name__ == "__main__":
    asyncio.run(run())
