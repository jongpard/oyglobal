# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import os
import math
import requests


def _fmt_price_line(row: dict) -> str:
    cur = float(row.get("price_current_usd", 0) or 0)
    org = float(row.get("price_original_usd", 0) or 0)
    disc = int(row.get("discount_rate_pct", 0) or 0)
    # 정수 반올림 할인율 유지
    parts = [f"US${cur:,.2f} (정가 US${org:,.2f})"]
    if disc > 0:
        parts.append(f"(↓{disc}%)")
    return " ".join(parts)


def post_top10_to_slack(csv_path: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        print("CSV_PATH / SLACK_WEBHOOK_URL 둘 다 필요합니다.")
        return

    top10 = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if len(top10) >= 10:
                break
            top10.append(row)

    if not top10:
        print("슬랙 전송: top10 데이터가 없습니다.")
        return

    # 헤더
    date_kst = top10[0].get("date_kst", "")
    title = f"올리브영 글로벌 전체 랭킹 ({date_kst} KST)\n\nTOP 10"

    lines = [title]
    for i, row in enumerate(top10, 1):
        # 요청사항: 브랜드명은 빼고, product_name만 사용
        name = row.get("product_name", "").strip()
        url = row.get("product_url", "").strip()
        price_line = _fmt_price_line(row)
        # 링크 형식
        if url:
            line = f"{i}. <{url}|{name}> – {price_line}"
        else:
            line = f"{i}. {name} – {price_line}"
        lines.append(line)

    payload = {"text": "\n".join(lines)}
    resp = requests.post(webhook, json=payload, timeout=15)
    resp.raise_for_status()
    print("Sent Slack message. status=", resp.status_code)
