import os
import glob
import pytz
import json
import requests
import pandas as pd
from datetime import datetime
from typing import Tuple, Optional

SEOUL = pytz.timezone("Asia/Seoul")

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/debug", exist_ok=True)

def get_kst_today_str() -> str:
    return datetime.now(SEOUL).strftime("%Y-%m-%d")

def save_today_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, encoding="utf-8-sig")

def _latest_csv_before(path_today: str) -> Optional[str]:
    today_name = os.path.basename(path_today)
    files = sorted(glob.glob("data/*_global.csv"))
    files = [f for f in files if os.path.basename(f) != today_name]
    return files[-1] if files else None

def load_previous_csv(path_today: str):
    prev_path = _latest_csv_before(path_today)
    if prev_path and os.path.exists(prev_path):
        try:
            return pd.read_csv(prev_path), prev_path
        except Exception:
            return None, prev_path
    return None, None

def _rank_merge(df_today: pd.DataFrame, df_prev: pd.DataFrame) -> pd.DataFrame:
    kt = df_today.copy(); kp = df_prev.copy()
    kt["key"] = kt["url"].fillna("") + "||" + kt["brand"].fillna("") + "||" + kt["name"].fillna("")
    kp["key"] = kp["url"].fillna("") + "||" + kp["brand"].fillna("") + "||" + kp["name"].fillna("")
    return pd.merge(kt[["rank","key","name","url"]], kp[["rank","key"]], on="key", how="left",
                    suffixes=("_today","_prev"))

def _fmt_rank_move(name: str, url: str, prev: str, today: str, delta: int) -> str:
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    delta_str = f"{arrow}{abs(delta)}" if delta != 0 else "±0"
    link = f"<{url}|{name}>" if url else name
    return f"- {link} {prev} → {today} ({delta_str})"

def compute_diffs_and_blocks(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame], prev_path: Optional[str]):
    today_top10 = df_today.nsmallest(10, "rank").copy()

    header = "*올리브영 글로벌몰 (US) 랭킹 리포트*"
    date_line = f"_기준: {get_kst_today_str()} (KST)_"
    blocks = [{"type":"section","text":{"type":"mrkdwn","text":f"{header}\n{date_line}"}}]

    lines = []
    for _, r in today_top10.iterrows():
        name = r.get("name",""); url = r.get("url","")
        line = f"- <{url}|{name}> {int(r['rank'])}위"
        if pd.notnull(r.get("discount_pct")): line += f" (↓{int(r['discount_pct'])}%)"
        if r.get("price_str"): line += f" · {r['price_str']}"
        lines.append(line)
    blocks.append({"type":"section","text":{"type":"mrkdwn","text":"*Top10*\n" + "\n".join(lines)}})

    if df_prev is None or df_prev.empty:
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":"_전일 데이터가 없어 비교 섹션은 건너뜁니다._"}})
        text = "\n".join(b["text"]["text"] for b in blocks if "text" in b)
        return blocks, text

    merged = _rank_merge(df_today, df_prev)
    merged["rank_prev"] = merged["rank_prev"].astype("Int64")
    movers = merged[merged["rank_prev"].notna()].copy()
    movers["delta"] = (movers["rank_prev"] - movers["rank_today"]).astype(int)

    up10 = movers.sort_values("delta", ascending=False).head(10)
    up_lines = [
        _fmt_rank_move(r["name"], r["url"], f"{int(r['rank_prev'])}위", f"{int(r['rank_today'])}위", int(r["delta"]))
        for _, r in up10.iterrows()
    ]
    blocks.append({"type":"section","text":{"type":"mrkdwn","text":"*급상승 TOP10*\n" + ("\n".join(up_lines) if up_lines else "_없음_")}})

    prev = df_prev.copy()
    prev["key"] = prev["url"].fillna("") + "||" + prev["brand"].fillna("") + "||" + prev["name"].fillna("")
    new_rankers = df_today[~df_today.apply(lambda r: (r["url"] or "") + "||" + (r["brand"] or "") + "||" + (r["name"] or "") in set(prev["key"]), axis=1)].copy()
    new_lines = [f"- <{r['url']}|{r['name']}> {int(r['rank'])}위" for _, r in new_rankers.head(10).iterrows()]
    blocks.append({"type":"section","text":{"type":"mrkdwn","text":"*뉴랭커*\n" + ("\n".join(new_lines) if new_lines else "_없음_")}})

    down5 = movers.sort_values("delta", ascending=True).head(5)
    down_lines = [
        _fmt_rank_move(r["name"], r["url"], f"{int(r['rank_prev'])}위", f"{int(r['rank_today'])}위", int(r["delta"]))
        for _, r in down5.iterrows()
    ]
    prev_top30 = df_prev.nsmallest(30, "rank").copy()
    today_top30 = df_today.nsmallest(30, "rank").copy()
    prev_top30["key"] = prev_top30["url"].fillna("") + "||" + prev_top30["brand"].fillna("") + "||" + prev_top30["name"].fillna("")
    today_top30["key"] = today_top30["url"].fillna("") + "||" + today_top30["brand"].fillna("") + "||" + today_top30["name"].fillna("")
    out_of_30 = prev_top30[~prev_top30["key"].isin(set(today_top30["key"]))].copy()
    out_lines = [f"- <{r['url']}|{r['name']}> {int(r['rank'])}위 → out" for _, r in out_of_30.iterrows()]

    blocks.append({"type":"section","text":{"type":"mrkdwn","text":"*급하락(5)*\n" + ("\n".join(down_lines) if down_lines else "_없음_")}})
    if out_lines:
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":"*상위 30위 → out*\n" + "\n".join(out_lines)}})

    ins = len(new_rankers)
    outs = len(df_prev[~df_prev["url"].isin(df_today["url"])])
    blocks.append({"type":"section","text":{"type":"mrkdwn","text":f"*랭크 인&아웃*\n- 인: {ins}개\n- 아웃: {outs}개"}})

    text = "\n".join(b["text"]["text"] for b in blocks if "text" in b)
    return blocks, text

# ===== Google Drive OAuth Uploader =====
def _oauth_token_from_refresh(client_id: str, client_secret: str, refresh_token: str) -> str:
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def gdrive_upload_oauth(local_path: str, folder_id: str, client_id: str, client_secret: str, refresh_token: str):
    if not os.path.exists(local_path):
        print("[WARN] GDrive upload skipped: file missing:", local_path); return
    try:
        token = _oauth_token_from_refresh(client_id, client_secret, refresh_token)
        meta = {"name": os.path.basename(local_path), "parents": [folder_id]}
        files = {
            "metadata": ("metadata", json.dumps(meta), "application/json; charset=UTF-8"),
            "file": (os.path.basename(local_path), open(local_path, "rb"), "text/csv"),
        }
        r = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers={"Authorization": f"Bearer {token}"},
            files=files,
            timeout=60,
        )
        if r.status_code not in (200, 201):
            print("[WARN] Google Drive 업로드 실패:", r.status_code, r.text[:200])
        else:
            file_id = r.json().get("id")
            print("[INFO] Google Drive 업로드 성공. fileId:", file_id)
    except Exception as e:
        print("[WARN] Google Drive 업로드 중 예외:", e)
