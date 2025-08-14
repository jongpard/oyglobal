# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import List, Dict

import pandas as pd

from oy_global import scrape_oliveyoung_global


DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True, parents=True)


def _save_csv(items: List[Dict]) -> str:
    if not items:
        return ""
    cols = [
        "date_kst",
        "rank",
        "brand",
        "product_name",
        "price_current_usd",
        "price_original_usd",
        "discount_rate_pct",
        "has_value_price",
        "product_url",
        "image_url",
    ]
    df = pd.DataFrame(items)[cols]

    # 숫자 컬럼 정리
    for c in ["price_current_usd", "price_original_usd", "discount_rate_pct"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    date_str = df["date_kst"].iloc[0]
    out = DATA_DIR / f"oliveyoung_global_{date_str}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return str(out)


async def run() -> None:
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return
    path = _save_csv(items)
    if path:
        print(f"📦 저장 완료: {path}")
        # 미리보기
        df = pd.read_csv(path)
        print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(run())
