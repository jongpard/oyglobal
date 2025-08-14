# slack_notify.py
import os, sys, json, requests
import pandas as pd
from datetime import datetime
import pytz

def fmt_money(x):
    return f"US${x:,.2f}" if pd.notna(x) else "-"

def main(csv_path: str):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        raise SystemExit("Missing env: SLACK_WEBHOOK_URL")

    df = pd.read_csv(csv_path)
    df = df.sort_values("rank").head(10)

    kst = pytz.timezone("Asia/Seoul")
    date_kst = datetime.now(kst).strftime("%Y-%m-%d")

    lines = []
    for _, r in df.iterrows():
        disc = ""
        if pd.notna(r["discount_rate_pct"]):
            disc = f" (↓{r['discount_rate_pct']:.2f}%)" if r["discount_rate_pct"] > 0 else ""
        line = (
            f"*#{int(r['rank'])}* <{r['product_url']}|{(r['product_name'])}> – "
            f"{fmt_money(r['price_current_usd'])}"
        )
        if pd.notna(r["price_original_usd"]) and r["price_original_usd"] != r["price_current_usd"]:
            line += f" (정가 {fmt_money(r['price_original_usd'])}){disc}"
        lines.append(line)

    text = "*올리브영 글로벌 Top 10* " + date_kst + " (KST)\n" + "\n".join(lines)

    resp = requests.post(webhook, json={"text": text})
    if resp.status_code >= 400:
        raise SystemExit(f"Slack error: {resp.status_code} {resp.text}")
    print("✅ Sent Slack message.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python slack_notify.py <csv_path>")
        sys.exit(1)
    main(sys.argv[1])
