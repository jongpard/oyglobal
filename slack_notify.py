# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
import os
import json
import urllib.request
from typing import List, Dict

def _fmt_money(v: float) -> str:
    # 소수 두 자리 고정
    return f"US${v:,.2f}"

def _build_lines(rows: List[Dict]) -> List[str]:
    lines = []
    for r in rows:
        name = r.get("product_name") or "(no name)"
        url  = r.get("product_url")   or "#"
        cur  = float(r.get("price_current_usd") or 0)
        org  = float(r.get("price_original_usd") or 0)
        pct  = int(r.get("discount_rate_pct") or 0)
        rank = int(r.get("rank") or 0)

        price_part = f"{_fmt_money(cur)}"
        if org and org > cur:
            price_part += f" (정가 {_fmt_money(org)})"
        disc_part = f"(↓{pct}% )" if pct > 0 else "(↓0%)"

        # 제품명만 링크. (브랜드 붙이지 않음)
        lines.append(f"{rank}. <{url}|{name}> – {price_part} {disc_part}")
    return lines

def post_top10_to_slack(csv_path: str) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not (webhook and csv_path and os.path.exists(csv_path)):
        print("CSV_PATH / SLACK_WEBHOOK_URL 둘 다 필요합니다.")
        return

    # CSV 상위 10개 로드
    rows: List[Dict] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for i, r in enumerate(rd):
            if i >= 10: break
            rows.append(r)

    title = f"올리브영 글로벌 전체 랭킹 ({rows[0]['date_kst']})" if rows else "올리브영 글로벌 전체 랭킹"
    body  = "*TOP 10*\n" + "\n".join(_build_lines(rows))

    payload = {
        "text": f"{title}\n{body}",
        "mrkdwn": True,
    }

    req = urllib.request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        print("Sent Slack message. status=", resp.status)
