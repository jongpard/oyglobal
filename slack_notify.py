# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

KST = timezone(timedelta(hours=9))


def _load_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _fmt_money(v) -> str:
    try:
        return f"US${float(v):.2f}"
    except Exception:
        return str(v)


def _fmt_disc_int(v) -> str:
    """
    할인율을 정수 %로 표시. 25.78 -> 26%
    """
    try:
        iv = int(round(float(v)))
        if iv == 0:
            return ""
        return f"(↓{iv}%)"
    except Exception:
        return ""


def _build_top10_text(rows: List[Dict], date_kst: str) -> str:
    """
    슬랙 메시지: '상품명만' 링크로 노출 (브랜드 제거)
    예) 1. <url|상품명> – US$25.99 (정가 US$30.00) (↓13%)
    """
    lines = []
    lines.append(f"*올리브영 글로벌 전체 랭킹 ({date_kst} KST)*")
    lines.append("")
    lines.append("*TOP 10*")

    for r in rows[:10]:
        rank = r.get("rank")
        name = (r.get("product_name") or "").strip()
        url = (r.get("product_url") or "").strip()

        cur = _fmt_money(r.get("price_current_usd", ""))
        org = _fmt_money(r.get("price_original_usd", ""))
        disc = _fmt_disc_int(r.get("discount_rate_pct", ""))

        line = f"{rank}. <{url}|{name}> – {cur}"
        if org and org.upper() != "US$0.00":
            line += f" (정가 {org})"
        if disc:
            line += f" {disc}"
        lines.append(line)

    return "\n".join(lines)


def _post_to_slack(text: str, webhook_url: str):
    payload = {"text": text}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        _ = resp.read()


def send_slack_message(csv_path: str, webhook_url: str):
    rows = _load_csv(csv_path)
    if not rows:
        raise RuntimeError("CSV가 비어 있습니다.")

    date_kst = rows[0].get("date_kst") or datetime.now(KST).strftime("%Y-%m-%d")
    text = _build_top10_text(rows, date_kst)
    _post_to_slack(text, webhook_url)


# ✅ 기존 코드 호환용: main.py 가 post_top10_to_slack 을 임포트해도 동작하도록 제공
def post_top10_to_slack(csv_path: str, webhook_url: str | None = None):
    """
    csv_path만 넘겨도 되도록 webhook_url 없으면 환경변수 사용.
    """
    webhook = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        raise RuntimeError("SLACK_WEBHOOK_URL 환경변수가 없습니다.")
    send_slack_message(csv_path, webhook)
