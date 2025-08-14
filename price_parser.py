import re
from typing import Dict, Optional

USD_RE = re.compile(r"US?\$\s*([0-9]+(?:\.[0-9]{1,2})?)", re.I)
VALUE_RE = re.compile(r"value\s*[:：]\s*US?\$\s*([0-9]+(?:\.[0-9]{1,2})?)", re.I)

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s)
    except:
        return None

def _find_all_usd(text: str):
    return [float(m.group(1)) for m in USD_RE.finditer(text)]

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 정가/할인가가 함께 있으면 둘 다 사용
      - 가격이 하나만 있으면 할인 없음(정가=현재가)
      - "Value: US$86" 존재 시 정가=Value, 현재가=첫 번째 USD
    """
    raw = price_block_text.replace("\n", " ")

    value_match = VALUE_RE.search(raw)
    value_price = _to_float(value_match.group(1)) if value_match else None

    usd_vals = _find_all_usd(raw)

    price_current = None
    price_original = None

    if value_price is not None:
        if usd_vals:
            price_current = usd_vals[0]
            price_original = value_price
        else:
            price_current = None
            price_original = value_price
    else:
        if len(usd_vals) >= 2:
            # 다양한 마크업을 고려해 휴리스틱 적용:
            price_current = min(usd_vals)
            price_original = max(usd_vals)
        elif len(usd_vals) == 1:
            price_current = usd_vals[0]
            price_original = usd_vals[0]
        else:
            price_current = None
            price_original = None

    discount = 0.0
    if price_current is not None and price_original and price_original > 0:
        discount = round((1 - (price_current / price_original)) * 100, 2)
        if discount < 0:
            discount = 0.0

    return {
        "price_current_usd": price_current,
        "price_original_usd": price_original if price_original is not None else price_current,
        "discount_rate_pct": discount if price_current is not None else None,
        "value_price_usd": value_price,
        "has_value_price": value_price is not None
    }
