# -*- coding: utf-8 -*-
import json
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import requests

KST = timezone(timedelta(hours=9))


def _arrow_and_pct(cur: float, org: float) -> str:
    if not org or org <= 0:
        return "(↓0%)"
    diff = (1 - (cur / org)) * 100.0
    pct = int(round(diff))
    arrow = "↓" if pct >= 0 else "↑"
    return f"({arrow}{abs(pct)}%)"


def post_top10_to_slack(csv_path: str, webhook_url: Optional[str] = None) -> None:
    """
    csv에서 상위 10개를 읽어 슬랙에 전송
    - 제품명만 링크로 출력(브랜드 중복 제거)
    - 할인율 정수(반올림)
    """
    df = pd.read_csv(csv_path)
    df = df.sort_values("rank").head(10)

    # 머리말
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lines = []
    lines.append(f"*OLIVE YOUNG Global*\n올리브영 글로벌 전체 랭킹 ({today} KST)\n")
    lines.append("TOP 10")

    for _, r in df.iterrows():
        name = str(r.get("product_name", "")).strip()
        cur = float(r.get("price_current_usd", 0))
        org = float(r.get("price_original_usd", 0))
        url = str(r.get("product_url", "")).strip()

        ap = _arrow_and_pct(cur, org)
        # 제품명만 링크로
        line = f"{int(r['rank'])}. <{url}|{name}> – US${cur:.2f} (정가 US${org:.2f}) {ap}"
        lines.append(line)

    text = "\n".join(lines)

    if not webhook_url:
        print("👉 Slack 미전송(웹훅 없음)\n" + text)
        return

    resp = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json; charset=utf-8"},
        data=json.dumps({"text": text}),
        timeout=15,
    )
    try:
        resp.raise_for_status()
    except Exception as e:
        print("Slack 전송 실패:", e)
        print(text)
