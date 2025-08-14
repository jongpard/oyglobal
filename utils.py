import os
import glob
import pytz
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
    # data/YYYY-MM-DD_global.csv 중 오늘 이전 가장 최신
    today_name = os.path.basename(path_today)
    files = sorted(glob.glob("data/*_global.csv"))
    if not files:
        return None
    # 오늘 파일 제거
    files = [f for f in files if os.path.basename(f) != today_name]
    if not files:
        return None
    return files[-1]

def load_previous_csv(path_today: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    prev_path = _latest_csv_before(path_today)
    if prev_path and os.path.exists(prev_path):
        try:
            return pd.read_csv(prev_path), prev_path
        except Exception:
            return None, prev_path
    return None, None

def _rank_merge(df_today: pd.DataFrame, df_prev: pd.DataFrame) -> pd.DataFrame:
    # 키: url 우선 → (brand+name)
    key_today = df_today.copy()
    key_prev = df_prev.copy()
    key_today["key"] = key_today["url"].fillna("") + "||" + key_today["brand"].fillna("") + "||" + key_today["name"].fillna("")
    key_prev["key"] = key_prev["url"].fillna("") + "||" + key_prev["brand"].fillna("") + "||" + key_prev["name"].fillna("")

    m = pd.merge(
        key_today[["rank", "key", "name", "url"]],
        key_prev[["rank", "key"]],
        on="key", how="left", suffixes=("_today", "_prev")
    )
    return m

def _fmt_rank_move(name: str, url: str, prev: str, today: str, delta: int) -> str:
    # 국내몰 포맷과 동일: "- 제품명 71위 → 7위 (↑64)"
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    delta_str = f"{arrow}{abs(delta)}" if delta != 0 else "±0"
    link = f"<{url}|{name}>" if url else name
    return f"- {link} {prev} → {today} ({delta_str})"

def compute_diffs_and_blocks(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame], prev_path: Optional[str]):
    # Top10
    today_top10 = df_today.nsmallest(10, "rank").copy()

    header = "*올리브영 글로벌몰 (US) 랭킹 리포트*"
    date_line = f"_기준: {get_kst_today_str()} (KST)_"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"{header}\n{date_line}"}}
    ]

    # Top10 섹션
    top10_lines = []
    for _, r in today_top10.iterrows():
        name = r.get("name", "")
        url = r.get("url", "")
        line = f"- <{url}|{name}> {int(r['rank'])}위"
        if pd.notnull(r.get("discount_pct")):
            line += f" (↓{int(r['discount_pct'])}%)"  # ↓는 '가격 할인율' 표식(혼동 방지: 문맥상 할인)
        if r.get("price_str"):
            line += f" · {r['price_str']}"
        top10_lines.append(line)
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*Top10*\n" + "\n".join(top10_lines)}})

    if df_prev is None or df_prev.empty:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "_전일 데이터가 없어 비교 섹션은 건너뜁니다._"}})
        return blocks, "\n".join(line for b in blocks for line in [b["text"]["text"]])

    # 비교계산
    merged = _rank_merge(df_today, df_prev)
    # 이동량 계산
    merged["rank_prev"] = merged["rank_prev"].astype("Int64")
    movers = merged[merged["rank_prev"].notna()].copy()
    movers["delta"] = (movers["rank_prev"] - movers["rank_today"]).astype(int)  # 양수면 상승

    # 급상승 상위 10
    up10 = movers.sort_values("delta", ascending=False).head(10)
    up_lines = []
    for _, r in up10.iterrows():
        up_lines.append(_fmt_rank_move(
            name=r["name"], url=r["url"],
            prev=f"{int(r['rank_prev'])}위", today=f"{int(r['rank_today'])}위",
            delta=int(r["delta"])
        ))
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*급상승 TOP10*\n" + ("\n".join(up_lines) if up_lines else "_없음_")}})

    # 뉴랭커: 오늘에만 존재
    today_keys = set(merged["key"])
    prev = df_prev.copy()
    prev["key"] = prev["url"].fillna("") + "||" + prev["brand"].fillna("") + "||" + prev["name"].fillna("")
    new_rankers = df_today[~df_today.apply(lambda r: (r["url"] or "") + "||" + (r["brand"] or "") + "||" + (r["name"] or "") in set(prev["key"]), axis=1)].copy()
    new_lines = [f"- <{r['url']}|{r['name']}> {int(r['rank'])}위" for _, r in new_rankers.head(10).iterrows()]
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*뉴랭커*\n" + ("\n".join(new_lines) if new_lines else "_없음_")}})

    # 급하락 5 (절대값 큰 하락)
    down5 = movers.sort_values("delta", ascending=True).head(5)
    down_lines = []
    for _, r in down5.iterrows():
        down_lines.append(_fmt_rank_move(
            name=r["name"], url=r["url"],
            prev=f"{int(r['rank_prev'])}위", today=f"{int(r['rank_today'])}위",
            delta=int(r["delta"])
        ))
    # 상위 30위에서 'out' 된 항목
    prev_top30 = df_prev.nsmallest(30, "rank")
    today_top30 = df_today.nsmallest(30, "rank")
    # 키 기반 비교
    prev_top30["key"] = prev_top30["url"].fillna("") + "||" + prev_top30["brand"].fillna("") + "||" + prev_top30["name"].fillna("")
    today_top30["key"] = today_top30["url"].fillna("") + "||" + today_top30["brand"].fillna("") + "||" + today_top30["name"].fillna("")
    out_of_30 = prev_top30[~prev_top30["key"].isin(set(today_top30["key"]))].copy()
    out_lines = [f"- <{r['url']}|{r['name']}> {int(r['rank'])}위 → out" for _, r in out_of_30.iterrows()]

    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*급하락(5)*\n" + ("\n".join(down_lines) if down_lines else "_없음_")}})
    if out_lines:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*상위 30위 → out*\n" + "\n".join(out_lines)}})

    # 랭크 인&아웃 (개수만)
    ins = len(new_rankers)
    outs = len(df_prev[~df_prev["url"].isin(df_today["url"])])
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*랭크 인&아웃*\n- 인: {ins}개\n- 아웃: {outs}개"}})

    text = "\n".join(b["text"]["text"] for b in blocks if "text" in b)
    return blocks, text

# (선택) Dropbox 업로드
def dropbox_upload_optional(token: str, local_path: str, remote_path: str):
    try:
        import requests, json, os
        with open(local_path, "rb") as f:
            data = f.read()
        headers = {
            "Authorization": f"Bearer {token}",
            "Dropbox-API-Arg": json.dumps({"path": remote_path, "mode": "overwrite"}),
            "Content-Type": "application/octet-stream",
        }
        r = requests.post("https://content.dropboxapi.com/2/files/upload", headers=headers, data=data, timeout=60)
        if r.status_code not in (200, 201):
            print("[WARN] Dropbox 업로드 실패:", r.status_code, r.text[:200])
        else:
            print("[INFO] Dropbox 업로드 성공:", remote_path)
    except Exception as e:
        print("[WARN] Dropbox 업로드 중 예외:", e)
