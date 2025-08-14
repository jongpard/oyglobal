# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import os
import requests


def _to_float(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "")
        return float(s) if s else default
    except Exception:
        return default


def _to_int_round(v, default: int = 0) -> int:
    try:
        return int(round(_to_float(v, float(default)), 0))
    except Exception:
        return default


def _fmt_price_line(row: dict) -> str:
    cur = _to_float(row.get("price_current_usd"), 0.0)
    org = _to_float(row.get("price_original_usd"), 0.0)
    disc = _to_int_round(row.get("discount_rate_pct"), 0)  # ← 정수 반올림

    parts = [f"US${cur:,.2f} (정가 US${org:,.2f})"]
    if disc > 0:
        parts.append(f"(↓{disc}%)")
    return " ".join(parts)


def post_top10_to_slack(csv_path: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        print("CSV_PATH / SLACK_WEBHOOK_URL 둘 다 필요합니다.")
        return

    if not os.path.exists(csv_path):
        print(f"슬랙 전송 생략: CSV가 없습니다. path={csv_path}")
        return

    top10 = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if len(top10) >= 10:
                break
            top10.append(row)

    if not top10:
        print("슬랙 전송 생략: top10 데이터가 비어 있습니다.")
        return

    date_kst = top10[0].get("date_kst", "")
    title = f"올리브영 글로벌 전체 랭킹 ({date_kst} KST)\n\nTOP 10"

    lines = [title]
    for i, row in enumerate(top10, 1):
        name = (row.get("product_name") or "").strip()  # 요청: 브랜드 제외
        url = (row.get("product_url") or "").strip()
        price_line = _fmt_price_line(row)
        line = f"{i}. <{url}|{name}> – {price_line}" if url else f"{i}. {name} – {price_line}"
        lines.append(line)

    payload = {"text": "\n".join(lines)}
    resp = requests.post(webhook, json=payload, timeout=15)
    resp.raise_for_status()
    print("Sent Slack message. status=", resp.status_code)
