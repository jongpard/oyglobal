# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import csv
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from oy_global import scrape_oliveyoung_global
from slack_notify import post_top10_to_slack

KST = timezone(timedelta(hours=9))


def _today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _save_csv(rows: List[Dict]) -> str:
    date_str = _today_kst()
    os.makedirs("data", exist_ok=True)
    path = f"data/oliveyoung_global_{date_str}.csv"

    # 컬럼 고정(브랜드/제품명 분리, 할인율 정수, 링크 & 이미지 포함)
    fields = [
        "date_kst",
        "rank",
        "brand",
        "product_name",
        "price_current_usd",
        "price_original_usd",
        "discount_rate_pct",
        "has_value_price",
        "product_url",
        "image_url",
    ]

    for r in rows:
        r["date_kst"] = date_str
        # 안전 가드
        r["discount_rate_pct"] = int(r.get("discount_rate_pct") or 0)

    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in rows:
            wr.writerow({k: r.get(k, "") for k in fields})

    print(f"📁 저장 완료: {path}")
    # 첫 10줄 프리뷰
    for r in rows[:10]:
        print(r["rank"], r["brand"], r["product_name"], r["price_current_usd"], r["price_original_usd"])
    return path


async def run() -> None:
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()  # List[dict] (최대 100)
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return
    csv_path = _save_csv(items)
    # 슬랙(있으면 전송)
    post_top10_to_slack(csv_path)


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
