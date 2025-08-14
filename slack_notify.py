import os
import csv
import json
from datetime import datetime, timezone, timedelta
import urllib.request

KST = timezone(timedelta(hours=9))

def fmt_price(v):
    try:
        n = float(v)
        return f"US${n:,.2f}"
    except:
        return "-"

def build_message(csv_path: str) -> str:
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    rows = sorted(rows, key=lambda x: int(x["rank"]))[:10]

    date_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M (KST)")
    lines = [f"*올리브영 글로벌 전체 랭킹* ({date_str})", "", "*TOP 10*"]
    for r in rows:
        rank = r["rank"]
        name = r["product_name"].strip()  # ✅ 상품명만 사용
        url = r["product_url"]
        cur = fmt_price(r["price_current_usd"])
        org = fmt_price(r["price_original_usd"])
        try:
            disc = float(r.get("discount_rate_pct") or 0.0)
        except:
            disc = 0.0
        disc_str = f"(↓{disc:.2f}%)" if disc > 0 else ""

        # 하이퍼링크는 상품명만
        line = f"{rank}. <{url}|{name}> – {cur} (정가 {org}) {disc_str}".rstrip()
        lines.append(line)

    lines.append("")
    lines.append("_(첫 실행이어서 비교 기준이 없습니다.)_")
    return "\n".join(lines)

def send_to_slack(text: str, webhook_url: str):
    data = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()

if __name__ == "__main__":
    csv_path = os.environ.get("CSV_PATH", "")
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not csv_path or not webhook:
        print("CSV_PATH / SLACK_WEBHOOK_URL 둘 다 필요합니다.")
        raise SystemExit(1)

    msg = build_message(csv_path)
    send_to_slack(msg, webhook)
    print("✅ Sent Slack message.")
