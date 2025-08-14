# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict
import urllib.request

KST = timezone(timedelta(hours=9))

def _fmt_money(v) -> str:
    try:
        return f"US${float(v):.2f}"
    except Exception:
        return str(v)

def _fmt_disc(v) -> str:
    try:
        return f"(↓{float(v):.2f}%)"
    except Exception:
        return ""

def _load_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _build_top10_text(rows: List[Dict], date_kst: str) -> str:
    """
    요구사항: 슬랙 메시지에는 '상품명만' 링크로 노출 (브랜드 제거)
    예: 1. <url|상품명> – US$25.99 (정가 US$30.00) (↓13.37%)
    """
    lines = []
    lines.append(f"*올리브영 글로벌 전체 랭킹 ({date_kst} KST)*")
    lines.append("")
    lines.append("*TOP 10*")

    for r in rows[:10]:
        rank = r.get("rank")
        name = r.get("product_name", "").strip()
        url = r.get("product_url", "").strip()

        cur = _fmt_money(r.get("price_current_usd", ""))
        org = _fmt_money(r.get("price_original_usd", ""))
        disc = _fmt_disc(r.get("discount_rate_pct", ""))

        line = f"{rank}. <{url}|{name}> – {cur}"
        if org and org.upper() != "US$0.00":
            line += f" (정가 {org})"
        if disc != "(↓0.00%)":
            line += f" {disc}"
        lines.append(line)

    return "\n".join(lines)

def send_slack_message(csv_path: str, webhook_url: str):
    rows = _load_csv(csv_path)
    if not rows:
        raise RuntimeError("CSV가 비어 있습니다.")

    date_kst = rows[0].get("date_kst") or datetime.now(KST).strftime("%Y-%m-%d")
    text = _build_top10_text(rows, date_kst)

    payload = {"text": text}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        _ = resp.read()

if __name__ == "__main__":
    # 예: python slack_notify.py data/oliveyoung_global_2025-08-15.csv $SLACK_WEBHOOK_URL
    import sys
    if len(sys.argv) < 3:
        print("CSV 경로와 Webhook URL 필요")
        raise SystemExit(1)
    send_slack_message(sys.argv[1], sys.argv[2])
