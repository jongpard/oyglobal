# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd

def _fmt_price(cur, org):
    if cur is None:
        return "US$0.00"
    s_cur = f"US${cur:.2f}"
    if org and org > cur:
        return f"{s_cur} (정가 US${org:.2f})"
    return s_cur

def _fmt_discount(pct):
    if pct is None:
        return ""
    return f"(↓{pct:.2f}%)"

def _mk_line(idx, row):
    text = f"<{row['product_url']}|{row['brand']} {row['product_name']}>"
    price = _fmt_price(row["price_current_usd"], row["price_original_usd"])
    disc = _fmt_discount(row["discount_rate_pct"])
    return f"{idx}. {text} – {price} {disc}"

def post_top10_to_slack(webhook_url: str, df_top10: pd.DataFrame) -> bool:
    need_cols = {"brand","product_name","price_current_usd","price_original_usd",
                 "discount_rate_pct","product_url","date_kst"}
    if not need_cols.issubset(set(df_top10.columns)):
        return False

    lines = [f"*올리브영 글로벌 전체 랭킹 ({df_top10.iloc[0]['date_kst']})*", "*TOP 10*"]
    for i, (_, r) in enumerate(df_top10.iterrows(), start=1):
        lines.append(_mk_line(i, r))

    payload = {"text": "\n".join(lines)}
    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        return resp.status_code // 100 == 2
    except Exception:
        return False
