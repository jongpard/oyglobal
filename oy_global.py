# ==== (oy_global.py) 여기부터 붙여넣기: 브랜드 정리 유틸 ====
import re
import math
import os
from datetime import datetime, timezone, timedelta
import pandas as pd

def _clean_spaces(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def _clean_brand_value(brand: str, product_name: str) -> str:
    """
    brand 칸 정리:
    - 여러 줄이면 첫 번째 비어있지 않은 줄만 사용
    - product_name 텍스트가 섞여 있으면 제거
    - 공백/개행 정리
    """
    if brand is None:
        return ""
    text = str(brand)

    # 첫 번째 비어있지 않은 줄만 사용
    first_non_empty = ""
    for part in text.splitlines():
        part = part.strip()
        if part:
            first_non_empty = part
            break
    text = first_non_empty or text.strip()

    # product_name 이 포함되어 있으면 제거
    if isinstance(product_name, str) and product_name:
        pn = re.sub(r"\s+", " ", product_name).strip()
        tt = re.sub(r"\s+", " ", text).strip()
        if pn and pn.lower() in tt.lower():
            tt = re.sub(re.escape(pn), "", tt, flags=re.I).strip()
            if tt:
                text = tt

    return _clean_spaces(text)

def _kst_today_str() -> str:
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime("%Y-%m-%d")

# ==== (oy_global.py) 여기부터 붙여넣기: CSV 저장 함수 교체 ====
def save_items_to_csv(items):
    """
    items(list of dict) -> DataFrame -> brand 정규화 -> CSV 저장.
    스크레이퍼 로직엔 손대지 않음.
    """
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(items)

    # brand 칼럼 정리 (브랜드명만 남도록)
    if "brand" in df.columns and "product_name" in df.columns:
        df["brand"] = df.apply(
            lambda r: _clean_brand_value(r.get("brand", ""), r.get("product_name", "")),
            axis=1,
        )

    # 컬럼 순서 정리(있는 것만 적용)
    wanted = [
        "date_kst", "rank", "brand", "product_name",
        "price_current_usd", "price_original_usd", "discount_rate_pct",
        "value_price_usd", "has_value_price",
        "product_url", "image_url",
    ]
    cols = [c for c in wanted if c in df.columns]
    if cols:
        df = df[cols]

    out_path = os.path.join("data", f"oliveyoung_global_{_kst_today_str()}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path
