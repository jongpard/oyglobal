# slack_notify.py
import os, sys, glob, requests
import pandas as pd
from datetime import datetime
import pytz

WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "").strip()

def money(x):
    return f"US${x:,.2f}" if pd.notna(x) else "-"

def load_latest_two():
    files = sorted(glob.glob("data/oliveyoung_global_*.csv"))
    if not files:
        raise SystemExit("No CSV files found in data/")
    latest = files[-1]
    prev = files[-2] if len(files) >= 2 else None
    return latest, prev

def safe_name(row):
    n = str(row.get("product_name", "") or "").strip()
    if n and n.lower() != "nan":
        return n
    b = str(row.get("brand", "") or "").strip()
    if b and b.lower() != "nan":
        return b
    # 마지막 수단: 링크의 prdtNo
    url = row.get("product_url", "")
    return f"상품({url.split('prdtNo=')[-1][:8]})" if "prdtNo=" in url else "상품"

def slack_post(text):
    if not WEBHOOK:
        print("No SLACK_WEBHOOK_URL; printing instead:\n", text)
        return
    resp = requests.post(WEBHOOK, json={"text": text})
    if resp.status_code >= 400:
        raise SystemExit(f"Slack error: {resp.status_code} {resp.text}")
    print("✅ Sent Slack message.")

def section_title(s): return f"*{s}*"

def main(csv_path=None):
    if csv_path is None:
        latest, prev = load_latest_two()
    else:
        latest = csv_path
        prev = None
        # prev 자동 탐색
        files = sorted(glob.glob("data/oliveyoung_global_*.csv"))
        if len(files) >= 2:
            prev = files[-2]

    df = pd.read_csv(latest)
    df["product_name"] = df.apply(safe_name, axis=1)
    df = df.sort_values("rank")
    df_top10 = df.head(10).copy()

    # --- Top10 ---
    kst = pytz.timezone("Asia/Seoul")
    date_kst = datetime.now(kst).strftime("%Y-%m-%d")
    lines = [section_title(f"올리브영 글로벌 전체 랭킹 ({date_kst} KST)"), ""]
    lines.append(section_title("TOP 10"))

    for _, r in df_top10.iterrows():
        cur = money(r["price_current_usd"])
        base = money(r["price_original_usd"])
        disc = ""
        if pd.notna(r["discount_rate_pct"]) and r["discount_rate_pct"] > 0:
            disc = f" (↓{r['discount_rate_pct']:.2f}%)"
        lines.append(f"{int(r['rank'])}. <{r['product_url']}|{r['product_name']}> – {cur} (정가 {base}){disc}")

    # --- Diff vs previous ---
    if prev and os.path.exists(prev):
        dfp = pd.read_csv(prev)
        dfp["product_name"] = dfp.apply(safe_name, axis=1)

        # 제품 식별은 URL 기준
        today = df.set_index("product_url")
        yesterday = dfp.set_index("product_url")

        # 공통 제품의 순위 변화
        inter = today.index.intersection(yesterday.index)
        diff = []
        for url in inter:
            cur = int(today.loc[url, "rank"])
            prv = int(yesterday.loc[url, "rank"])
            delta = prv - cur  # +: 상승, -: 하락
            if delta != 0:
                diff.append((url, cur, prv, delta, today.loc[url, "product_name"]))
        # 급상승(내림차순), 급하락(오름차순)
        up = sorted([d for d in diff if d[3] > 0], key=lambda x: -x[3])[:10]
        down = sorted([d for d in diff if d[3] < 0], key=lambda x: x[3])[:5]

        # 뉴랭커/랭크아웃
        ins = list(today.index.difference(yesterday.index))
        outs = list(yesterday.index.difference(today.index))

        if up:
            lines += ["", section_title("급상승")]
            for url, cur, prv, d, name in up:
                lines.append(f"- {name} {prv}위 → {cur}위 (↑{d})")

        if ins:
            lines += ["", section_title("뉴랭커")]
            for url in ins[:10]:
                name = today.loc[url, "product_name"]
                cur = int(today.loc[url, "rank"])
                lines.append(f"- {name} NEW → {cur}위")

        if down:
            lines += ["", section_title("급하락")]
            for url, cur, prv, d, name in down:
                lines.append(f"- {name} {prv}위 → {cur}위 (↓{abs(d)})")

        # 인&아웃 갯수만
        if ins or outs:
            lines += ["", section_title("랭크 인&아웃")]
            lines.append(f"{len(ins) + len(outs)}개의 제품이 인&아웃 되었습니다. (IN {len(ins)} / OUT {len(outs)})")

    text = "\n".join(lines)
    slack_post(text)

if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else None
    main(csv)
