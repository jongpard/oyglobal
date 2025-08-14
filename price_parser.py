# price_parser.py
import re
from typing import Dict, Optional, List

# HTML 태그 제거 (innerHTML 섞일 수 있음)
TAG_RE = re.compile(r"<[^>]+>")

def _strip_tags(s: str) -> str:
    return TAG_RE.sub(" ", s or "")

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ")
    s = s.replace("\n", " ")
    return s

# 금액 후보: 통화기호 유무 허용 + 천단위 콤마 허용
# 퍼센트 뒤/앞 숫자는 제외 (37% 같은 것 배제)
AMOUNT_RE = re.compile(
    r"(?<![#0-9])"                # 해시태그/연속숫자 앞 배제
    r"(?:US?\$)?\s*"              # 통화기호 선택
    r"("                          # 캡처 시작
    r"(?:\d{1,3}(?:,\d{3})*|\d+)" # 정수부(천단위 콤마 허용)
    r"(?:\.\d{1,2})?"             # 소수부 선택
    r")"
    r"(?!\s*%)",                  # 퍼센트 금지
    re.I
)

# Value: US$86.00 패턴 (콜론/공백 변형 허용)
VALUE_RE = re.compile(
    r"value\s*[:：]?\s*(?:US?\$)?\s*("
    r"(?:\d{1,3}(?:,\d{3})*|\d+)"
    r"(?:\.\d{1,2})?"
    r")",
    re.I
)

def _to_float(x: str) -> Optional[float]:
    try:
        return float((x or "").replace(",", ""))
    except:
        return None

def _find_amounts(text: str) -> List[float]:
    vals: List[float] = []
    for m in AMOUNT_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is None:
            continue
        # 합리적 범위 필터: 0.5 ~ 1000 USD
        # (0, 추적코드, 비정상 큰 수 제거)
        if 0.5 <= v <= 1000:
            vals.append(v)
    return vals

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 'Value'가 있으면 정가=Value, 현재가=첫 번째 금액
      - 금액이 2개 이상이면 min=현재가, max=정가
      - 금액이 1개면 할인 없음(정가=현재가)
    """
    raw = _norm(price_block_text)
    # 텍스트 먼저 → 필요시 태그 제거본도 사용
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
