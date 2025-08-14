# slack_notify.py
import os
import glob
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import pandas as pd
import requests

DATA_DIR = "data"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

RISE_TH = int(os.getenv("RISE_TH", "30"))     # 급상승 +30
DROP_TH = -int(os.getenv("DROP_TH", "30"))    # 급하락 -30


# ---------- utils ----------
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


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    if "discount_rate_pct" not in df.columns and \
       "price_current_usd" in df.columns and "price_original_usd" in df.columns:
        cur = pd.to_numeric(df["price_current_usd"], errors="coerce")
        ori = pd.to_numeric(df["price_original_usd"], errors="coerce")
        df["discount_rate_pct"] = ((1 - (cur/ori)) * 100).round(2)
    return df


def fmt_money(v):
    if pd.isna(v):
        return "—"
    try:
        return f"US${float(v):,.2f}"
    except Exception:
        return f"US${v}"


def shorten(s, n=95):
    t = (str(s or "").replace("\n", " ").replace("\r", " ").strip())
    t = re.sub(r"\s+", " ", t)
    return t if len(t) <= n else t[: n - 1] + "…"


def safe_label(text):
    if not text:
        return ""
    return str(text).replace("|", "¦").replace(">", "›")


def slack_link(url, label):
    label = safe_label(label)
    if url and isinstance(url, str) and url.startswith("http"):
        return f"<{url}|{label}>"
    return label


# product_name만 사용(비어있을 때만 brand 사용)
def title_from_row(row: Dict, prefer_today=True) -> str:
    if prefer_today:
        name = row.get("name_t") if "name_t" in row else row.get("product_name")
        brand = row.get("brand_t") if "brand_t" in row else row.get("brand")
    else:
        name = row.get("name_p")
        brand = row.get("brand_p")
    title = (name or brand or "").strip()
    title = re.sub(r"\s+", " ", title)
    title = shorten(title, 120)
    return slack_link(row.get("product_url"), title)


# ---------- sections ----------
def build_top10(df: pd.DataFrame) -> str:
    out = []
    for _, r in df.sort_values("rank").head(10).iterrows():
        title = title_from_row(r)  # product_name만
        price = fmt_money(r.get("price_current_usd"))
        ori = fmt_money(r.get("price_original_usd"))
        disc = r.get("discount_rate_pct")
        disc_txt = f"(↓{float(disc):.2f}%)" if pd.notna(disc) else ""
        out.append(f"{int(r['rank'])}. {title} – {price} (정가 {ori}) {disc_txt}".strip())
    return "\n".join(out)


def analyze(df_today: pd.DataFrame, df_prev: pd.DataFrame) -> Dict[str, List[str]]:
    cols = ["product_url", "rank", "brand", "product_name"]
    t = df_today[cols].rename(columns={"rank": "rank_today", "brand": "brand_t", "product_name": "name_t"})
    p = df_prev[cols].rename(columns={"rank": "rank_prev", "brand": "brand_p", "product_name": "name_p"})
    m = pd.merge(t, p, on="product_url", how="outer")

    new_mask = m["rank_prev"].isna() & m["rank_today"].notna()
    out_mask = m["rank_today"].isna() & m["rank_prev"].notna()
    stay_mask = m["rank_today"].notna() & m["rank_prev"].notna()

    stay = m[stay_mask].copy()
    stay["delta"] = stay["rank_prev"] - stay["rank_today"]  # +: 상승, -: 하락

    up = stay[stay["delta"] >= RISE_TH].sort_values(["delta", "rank_today"], ascending=[False, True])
    down_in = stay[stay["delta"] <= DROP_TH].sort_values(["delta", "rank_today"])
    out_top30 = m[out_mask & (m["rank_prev"] <= 30)].copy().sort_values("rank_prev")
    newcomers = m[new_mask].copy().sort_values("rank_today")

    ins_cnt = int(new_mask.sum())
    outs_cnt = int(out_mask.sum())
    inout_total = ins_cnt + outs_cnt

    def row_up(r):
        return f"- {title_from_row(r, True)} {int(r['rank_prev'])}위 → {int(r['rank_today'])}위 (↑{int(r['delta'])})"

    def row_down(r):
        return f"- {title_from_row(r, True)} {int(r['rank_prev'])}위 → {int(r['rank_today'])}위 (↓{abs(int(r['delta']))})"

    def row_new(r):
        return f"- {title_from_row(r, True)} NEW → {int(r['rank_today'])}위"

    def row_out(r):
        return f"- {title_from_row(r, False)} {int(r['rank_prev'])}위 → OUT"

    lines_up = [row_up(r) for _, r in up.head(10).iterrows()]
    lines_new = [row_new(r) for _, r in newcomers.head(10).iterrows()]
    lines_down = [row_down(r) for _, r in down_in.head(10).iterrows()]
    lines_out = [row_out(r) for _, r in out_top30.head(10).iterrows()]

    if lines_out:
        if lines_down:
            lines_down.append("— 어제 TOP30 → OUT —")
        lines_down.extend(lines_out)

    return {
        "up": lines_up,
        "new": lines_new,
        "down": lines_down,
        "inout": f"{inout_total}개의 제품이 인&아웃 되었습니다.",
    }


def post_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[WARN] SLACK_WEBHOOK_URL 미설정. 콘솔 출력만 합니다.\n")
        print(text)
        return
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
    resp.raise_for_status()


# ---------- main ----------
def main():
    latest, prev = find_latest_prev()
    if not latest:
        print("data/에 CSV가 없습니다.")
        return

    df_today = load_csv(latest)

    header = f"*올리브영 글로벌 전체 랭킹 ({kst_date_str()} KST)*"
    top10 = "*TOP 10*\n" + build_top10(df_today)

    if prev:
        df_prev = load_csv(prev)
        res = analyze(df_today, df_prev)

        parts = [header, top10]
        if res["up"]:
            parts.append("\n🥇 *급상승* (↑30 이상)")
            parts.extend(res["up"])
        if res["new"]:
            parts.append("\n🆕 *뉴랭커*")
            parts.extend(res["new"])
        if res["down"]:
            parts.append("\n🔻 *급하락* (↓30 이상 & 어제 TOP30→OUT 포함)")
            parts.extend(res["down"])
        parts.append(f"\n🔁 *랭크 인&아웃*\n{res['inout']}")
        msg = "\n".join(parts)
    else:
        msg = f"{header}\n\n{top10}\n\n(첫 실행이어서 비교 기준이 없습니다.)"

    post_slack(msg)
    print("Sent Slack message.")


if __name__ == "__main__":
    main()
