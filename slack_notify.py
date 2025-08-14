# slack_notify.py
import os
import json
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict

KST = timezone(timedelta(hours=9))

def _fmt_money(v: float) -> str:
    return f"US${v:.2f}"

def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"

def build_slack_text(date_kst: datetime, top10: List[Dict], prev_top10: List[Dict] | None) -> str:
    header = ":olive_oil: *OLIVE YOUNG Global*  \n*올리브영 글로벌몰 (US) 랭킹 리포트*\n기준: " \
             f"{date_kst.strftime('%Y-%m-%d (KST)')}\n\n*Top10*"

    lines = []
    for r in top10[:10]:
        # 한 줄로만 출력 (강제 줄바꿈 없음)
        line = (f"{r['rank']}. {r['name']} – {_fmt_money(r['sale_price'])} "
                f"(정가 {_fmt_money(r['original_price'])}) (↓{_fmt_pct(r['discount_pct'])})")
        lines.append(line)

    body = "\n".join(lines)

    if not prev_top10:
        tail = "\n\n전일 데이터가 없어 비교 섹션은 건너뜁니다."
        return f"{header}\n{body}{tail}"

    # 비교 섹션 (필요 시 확장)
    return f"{header}\n{body}"

def post_to_slack(webhook_url: str, text: str) -> None:
    payload = {"text": text}
    resp = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
    try:
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Slack 전송 실패: {e}")
    else:
        print("[INFO] Slack 전송 성공")
