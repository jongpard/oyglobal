# price_parser.py
import re
from typing import Dict, Optional, List

TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://\S+")
PRDTNO_RE = re.compile(r"prdtNo=\w+", re.I)
# value="...": HTML 속성 값 제거용 (오탐 방지)
VALUE_ATTR_RE = re.compile(r'value\s*=\s*"(?:[^"\\]|\\.)*"|value\s*=\s*\'(?:[^\'\\]|\\.)*\'', re.I)

def _strip_tags(s: str) -> str:
    return TAG_RE.sub(" ", s or "")

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ").replace("\n", " ")
    return s

# 통화 기호가 붙은 금액(두 자리 소수 필수)
CURR_RE = re.compile(r"US?\$\s*((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2})", re.I)

# 통화 기호 없는 금액(두 자리 소수만) – CSS로 'US$'가 붙는 경우 대비
PLAIN2_RE = re.compile(r"\b((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2})\b")

# 진짜 텍스트 “Value:”만 매칭 (value= 속성은 제외)
VALUE_RE = re.compile(
    r"(?<![A-Za-z0-9_])value(?!\s*=)\s*[:：]?\s*(?:US?\$)?\s*((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2})",
    re.I,
)

def _to_float(x: str) -> Optional[float]:
    try:
        return float((x or "").replace(",", ""))
    except:
        return None

def _find_amounts(text: str) -> List[float]:
    vals: List[float] = []
    for m in CURR_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is not None:
            vals.append(v)
    for m in PLAIN2_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is not None:
            vals.append(v)
    # 합리적 범위만 유지
    return [v for v in vals if 0.5 <= v <= 1000]

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 텍스트 내에서 'Value:' 가 보이면 정가 = Value, 현재가 = 첫 번째 금액
      - 금액이 2개 이상이면 현재가 = min, 정가 = max
      - 금액이 1개면 할인 없음(정가=현재가)
    """
    raw = _norm(price_block_text)
    # 가격과 무관한 숫자 소스 제거
    raw = URL_RE.sub(" ", raw)
    raw = PRDTNO_RE.sub(" ", raw)
    raw = VALUE_ATTR_RE.sub(" ", raw)

    # 1차: 텍스트에서 파싱
    vm = VALUE_RE.search(raw)
    value_price = _to_float(vm.group(1)) if vm else None
    amounts = _find_amounts(raw)

    # 2차: 태그 제거본에서도 한 번 더 시도
    if not amounts:
        raw2 = _strip_tags(raw)
        if value_price is None:
            vm2 = VALUE_RE.search(raw2)
            value_price = _to_float(vm2.group(1)) if vm2 else None
        amounts = _find_amounts(raw2)

    price_current = price_original = None
    if value_price is not None and amounts:
        price_current = amounts[0]
        price_original = value_price
    elif len(amounts) >= 2:
        price_current = min(amounts)
        price_original = max(amounts)
    elif len(amounts) == 1:
        price_current = amounts[0]
        price_original = amounts[0]

    discount = None
    if price_current is not None and price_original and price_original > 0:
        d = round((1 - (price_current / price_original)) * 100, 2)
        discount = 0.0 if d < 0 else d

    return {
        "price_current_usd": price_current,
        "price_original_usd": price_original if price_original is not None else price_current,
        "discount_rate_pct": discount,
        "value_price_usd": value_price,
        "has_value_price": value_price is not None,
    }
