import re
from typing import Dict, Optional, List

# 태그 제거용
TAG_RE = re.compile(r"<[^>]+>")

# 통화기호 유무/천단위 콤마/소수점 0~2 모두 허용
# 예: US$31.86, 31.86, 1,234.5, 28, 54.00 등
AMOUNT_RE = re.compile(
    r"(?:US?\$)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)",
    re.I,
)
# Value: US$86.00 같은 표기(콜론/공백 변형 허용)
VALUE_RE = re.compile(
    r"value\s*[:：]?\s*(?:US?\$)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)",
    re.I,
)

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except:
        return None

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ")
    s = s.replace("\n", " ")
    return s

def _strip_tags(html: str) -> str:
    return TAG_RE.sub(" ", html or "")

def _find_amounts(text: str) -> List[float]:
    return [_to_float(m.group(1)) for m in AMOUNT_RE.finditer(text) if _to_float(m.group(1)) is not None]

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    우선 텍스트에서 시도 → 실패 시 HTML 문자열에서도 시도.
    규칙:
      - 'Value'가 있으면 정가=Value, 현재가=첫 번째 금액
      - 금액이 2개 이상이면 min=현재가, max=정가
      - 금액이 1개면 할인 없음(정가=현재가)
    """
    raw = _norm(price_block_text)

    # 1차: 텍스트에서 파싱
    value_match = VALUE_RE.search(raw)
    value_price = _to_float(value_match.group(1)) if value_match else None
    amounts = _find_amounts(raw)

    # 텍스트에서 아무 금액도 못 찾으면 HTML 태그 제거 버전도 한 번 더
    if not amounts:
        raw2 = _strip_tags(raw)
        value_match = VALUE_RE.search(raw2) or value_match
        if value_match and value_price is None:
            value_price = _to_float(value_match.group(1))
        amounts = _find_amounts(raw2)

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
        "has_value_price": value_price is not None,
    }
