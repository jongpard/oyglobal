# price_parser.py
import re
from typing import Dict, Optional, List

TAG_RE = re.compile(r"<[^>]+>")

def _strip_tags(s: str) -> str:
    return TAG_RE.sub(" ", s or "")

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ")
    s = s.replace("\n", " ")
    return s

# 통화기호가 있는 금액 (두 자리 소수 허용)
CURR_RE = re.compile(
    r"US?\$\s*(" r"(?:\d{1,3}(?:,\d{3})*|\d+)" r"(?:\.\d{2})" r")",
    re.I,
)

# 통화기호 없는 금액은 반드시 두 자리 소수만 허용(평점 4.8 등 제거)
PLAIN2_RE = re.compile(
    r"\b(" r"(?:\d{1,3}(?:,\d{3})*|\d+)" r"(?:\.\d{2})" r")\b",
    re.I,
)

# Value: US$86.00 (두 자리 소수)
VALUE_RE = re.compile(
    r"value\s*[:：]?\s*(?:US?\$)?\s*(" r"(?:\d{1,3}(?:,\d{3})*|\d+)" r"(?:\.\d{2})" r")",
    re.I,
)

URL_RE = re.compile(r"https?://\S+")
PRDTNO_RE = re.compile(r"prdtNo=\w+", re.I)

def _to_float(x: str) -> Optional[float]:
    try:
        return float((x or "").replace(",", ""))
    except:
        return None

def _find_amounts(text: str) -> List[float]:
    vals: List[float] = []

    # 1) 통화 기호가 붙은 금액 우선
    for m in CURR_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is not None:
            vals.append(v)

    # 2) 통화 기호 없는 금액(반드시 소수점 둘째자리)
    for m in PLAIN2_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is None:
            continue
        vals.append(v)

    # 합리적 범위만 남김 (0.5 ~ 1000 USD)
    vals = [v for v in vals if 0.5 <= v <= 1000]
    return vals

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 'Value'가 있으면 정가=Value, 현재가=첫 번째 금액
      - 금액이 2개 이상이면 min=현재가, max=정가
      - 금액이 1개면 할인 없음(정가=현재가)
    """
    raw = _norm(price_block_text)
    # URL/상품번호 등 숫자 제거
    raw = URL_RE.sub(" ", raw)
    raw = PRDTNO_RE.sub(" ", raw)
    # 텍스트 먼저 → 실패 시 태그 제거본
    value_match = VALUE_RE.search(raw)
    value_price = _to_float(value_match.group(1)) if value_match else None
    amounts = _find_amounts(raw)

    if not amounts:
        raw2 = _strip_tags(raw)
        if value_price is None:
            vm2 = VALUE_RE.search(raw2)
            value_price = _to_float(vm2.group(1)) if vm2 else None
        amounts = _find_amounts(raw2)

    price_current = None
    price_original = None

    if value_price is not None and amounts:
        price_current = amounts[0]
        price_original = value_price
    elif len(amounts) >= 2:
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
