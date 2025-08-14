import re
from typing import Dict, Optional

# US$가 CSS로 붙는 경우를 대비해, 통화기호가 없어도 "두 자리 소수" 패턴을 가격으로 인식
# 예: "18.75", "31.50" 등. (1+1, 80 같은 숫자는 배제됨)
AMOUNT_RE = re.compile(r"(?:US?\$)?\s*([0-9]+(?:\.[0-9]{2}))", re.I)
VALUE_RE = re.compile(r"value\s*[:：]?\s*(?:US?\$)?\s*([0-9]+(?:\.[0-9]{2}))", re.I)

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s)
    except:
        return None

def _find_all_amounts(text: str):
    return [float(m.group(1)) for m in AMOUNT_RE.finditer(text)]

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 'Value'가 보이면 정가=Value, 현재가=첫 번째 금액
      - 정가/할인가 2개 이상 보이면 min=현재가, max=정가
      - 하나만 보이면 할인 없음(정가=현재가)
    """
    raw = (price_block_text or "").replace("\n", " ")

    value_match = VALUE_RE.search(raw)
    value_price = _to_float(value_match.group(1)) if value_match else None

    amounts = _find_all_amounts(raw)

    price_current = None
    price_original = None

    if value_price is not None:
        if amounts:
            price_current = amounts[0]
            price_original = value_price
        else:
            price_current = None
            price_original = value_price
    else:
        if len(amounts) >= 2:
            price_current = min(amounts)
            price_original = max(amounts)
        elif len(amounts) == 1:
            price_current = amounts[0]
            price_original = amounts[0]
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
