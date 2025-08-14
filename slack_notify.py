# slack_notify.py
import os
import glob
import requests
from datetime import datetime, timezone, timedelta
import pandas as pd

DATA_DIR = "data"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# 임계값: 급상승/급하락 30칸
RISE_TH = int(os.getenv("RISE_TH", "30"))      # >= +30
DROP_TH = -int(os.getenv("DROP_TH", "30"))     # <= -30

def kst_date_str():
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime("%Y-%m-%d")

def find_latest_prev():
    files = sorted(glob.glob(f"{DATA_DIR}/oliveyoung_global_*.csv"))
    if not files:
        return None, None
    latest = files[-1]
    prev = files[-2] if len(files) >= 2 else None
    return latest, prev

def load_csv(path):
    df = pd.read_csv(path)
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    return df

def fmt_money(v):
    if pd.isna(v):
        return "—"
    return f"US${v:,.2f}"

def shorten(s, n=110):
    t = (str(s or "").strip()).replace("\n", " ")
    return t if len(t) <= n else t[: n - 1] + "…"

def build_top10(df):
    lines = []
    for _, r in df.sort_values("rank").head(10).iterrows():
        brand = str(r.get("brand") or "").strip()
        name  = shorten(r.get("product_name"))
        price = r.get("price_current_usd")
        ori   = r.get("price_original_usd")
        disc  = r.get("discount_rate_pct")
        disc_txt = f"(↓{disc:.2f}%)" if pd.notna(disc) else ""
        show = f"{brand} {name}".strip()
        lines.append(f"{int(r['rank'])}. {show} – {fmt_money(price)} (정가 {fmt_money(ori)}) {disc_txt}")
    return "\n".join(lines)

def analyze(df_today, df_prev):
    cols = ["product_url", "rank", "brand", "product_name"]
    t = df_today[cols].rename(columns={"rank": "rank_today", "brand": "brand_t", "product_name": "name_t"})
    p = df_prev[cols].rename(columns={"rank": "rank_prev",  "brand": "brand_p", "product_name": "name_p"})

    m = pd.merge(t, p, on="product_url", how="outer")

    new_mask  = m["rank_prev"].isna() & m["rank_today"].notna()
    out_mask  = m["rank_today"].isna() & m["rank_prev"].notna()
    stay_mask = m["rank_today"].notna() & m["rank_prev"].notna()

    # 급상승: +30 이상
    stay = m[stay_mask].copy()
    stay["delta"] = stay["rank_prev"] - stay["rank_today"]  # +면 상승
    up = stay[stay["delta"] >= RISE_TH].sort_values(["delta", "rank_today"], ascending=[False, True])

    # 급하락: -30 이상 하락 OR 어제 TOP30이었는데 오늘 OUT
    down_in = stay[stay["delta"] <= DROP_TH].sort_values(["delta", "rank_today"])  # delta 가장 작은(음수 큰) 순
    out_top30 = m[out_mask & (m["rank_prev"] <= 30)].copy().sort_values("rank_prev")  # 어제 30위 내 → OUT

    # 뉴랭커: 오늘만 있고 어제는 없음
    newcomers = m[new_mask].copy().sort_values("rank_today")

    # 랭크 인&아웃 개수
    ins_cnt = int(new_mask.sum())
    outs_cnt = int(out_mask.sum())
    inout_total = ins_cnt + outs_cnt

    def row_text_up(r):
        brand = (r.get("brand_t") or r.get("brand_p") or "").strip()
        name  = shorten(r.get("name_t") or r.get("name_p") or "")
        prev  = int(r["rank_prev"])
        now   = int(r["rank_today"])
        d     = int(r["delta"])
        return f"- {brand} {name} {prev}위 → {now}위 (↑{d})"

    def row_text_down(r):
        brand = (r.get("brand_t") or r.get("brand_p") or "").strip()
        name  = shorten(r.get("name_t") or r.get("name_p") or "")
        prev  = int(r["rank_prev"])
        now   = int(r["rank_today"])
        d     = int(r["delta"])
        return f"- {brand} {name} {prev}위 → {now}위 (↓{abs(d)})"

    def row_text_new(r):
        brand = (r.get("brand_t") or r.get("brand_p") or "").strip()
        name  = shorten(r.get("name_t") or r.get("name_p") or "")
        now   = int(r["rank_today"])
        return f"- {brand} {name} NEW → {now}위"

    def row_text_out(r):
        brand = (r.get("brand_t") or r.get("brand_p") or "").strip()
        name  = shorten(r.get("name_t") or r.get("name_p") or "")
        prev  = int(r["rank_prev"])
        return f"- {brand} {name} {prev}위 → OUT"

    lines_up   = [row_text_up(r)   for _, r in up.head(10).iterrows()]
    lines_new  = [row_text_new(r)  for _, r in newcomers.head(10).iterrows()]
    lines_down = [row_text_down(r) for _, r in down_in.head(10).iterrows()]
    lines_out  = [row_text_out(r)  for _, r in out_top30.head(10).iterrows()]

    return {
        "up": lines_up,
        "new": lines_new,
        "down": lines_down + (["— 어제 TOP30 → OUT —"] if len(lines_out)>0 and len(lines_down)>0 else []) + lines_out,
        "inout": f"{inout_total}개의 제품이 인&아웃 되었습니다."
    }

def post_slack(text):
    if not SLACK_WEBHOOK_URL:
        print("[WARN] SLACK_WEBHOOK_URL 미설정. 메시지 출력만 합니다.\n")
        print(text)
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
    resp.raise_for_status()

def main():
    latest, prev = find_latest_prev()
    if not latest:
        print("data/ 에 CSV가 없습니다.")
        return

    df_today = load_csv(latest)

    header = f"*올리브영 글로벌 전체 랭킹 ({kst_date_str()} KST)*"
    top10  = "*TOP 10*\n" + build_top10(df_today)

    if prev:
        df_prev = load_csv(prev)
        res = analyze(df_today, df_prev)

        blocks = [header, top10]
        if res["up"]:
            blocks.append("\n*급상승 (↑30 이상)*")
            blocks.extend(res["up"])
        if res["new"]:
            blocks.append("\n*뉴랭커*")
            blocks.extend(res["new"])
        if res["down"]:
            blocks.append("\n*급하락 (↓30 이상 & 어제 TOP30→OUT 포함)*")
            blocks.extend(res["down"])
        blocks.append(f"\n*랭크 인&아웃*\n{res['inout']}")
        msg = "\n".join(blocks)
    else:
        msg = f"{header}\n\n{top10}\n\n(첫 실행이어서 비교 기준이 없습니다.)"

    post_slack(msg)
    print("Sent Slack message.")

if __name__ == "__main__":
    main()
