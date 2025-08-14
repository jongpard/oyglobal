# price_parser.py
import re
from typing import Dict, Optional, List

# ----- 텍스트 정리 -----
TAG_RE        = re.compile(r"<[^>]+>")
URL_SCHEME_RE = re.compile(r"https?://\S+|//\S+", re.I)      # http/https + 스킴없는 URL까지
ATTR_URL_RE   = re.compile(r'(?:src|href)\s*=\s*(?:"[^"]+"|\'[^\']+\')', re.I)
PRDTNO_RE     = re.compile(r"prdtNo=\w+", re.I)
VALUE_ATTR_RE = re.compile(r'value\s*=\s*"(?:[^"\\]|\\.)*"|value\s*=\s*\'(?:[^\'\\]|\\.)*\'', re.I)
LONGNUM_RE    = re.compile(r"\d{5,}")  # 가격과 무관한 장문 숫자 제거(이미지ID 등)

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ").replace("\n", " ")
    return s

def _strip_noise(text: str) -> str:
    """가격과 무관한 소스(링크/속성/장문숫자 등) 제거."""
    t = _norm(text)
    t = URL_SCHEME_RE.sub(" ", t)
    t = ATTR_URL_RE.sub(" ", t)
    t = PRDTNO_RE.sub(" ", t)
    t = VALUE_ATTR_RE.sub(" ", t)
    t = TAG_RE.sub(" ", t)
    t = LONGNUM_RE.sub(" ", t)
    return re.sub(r"\s+", " ", t).strip()

# ----- 금액 패턴 -----
# US$ 가 붙은 금액(두 자리 소수 필수)
CURR_RE  = re.compile(r"US?\$\s*((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2})", re.I)
# 통화기호 없는 금액(두 자리 소수 필수) — CSS로 기호가 붙는 케이스 대응
PLAIN_RE = re.compile(r"\b((?:\d{1,3}(?:,\d{3})*|\d+)\.\d{2})\b")
# 진짜 텍스트 "Value:" 만 (value= 속성 제외)
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
    # 통화기호 없는 금액은 보조로만 사용
    for m in PLAIN_RE.finditer(text):
        v = _to_float(m.group(1))
        if v is not None:
            vals.append(v)
    # 합리적 범위만 (0.5 ~ 500 USD)
    vals = [v for v in vals if 0.5 <= v <= 500]
    return vals

def parse_prices_and_discount(price_block_text: str) -> Dict:
    """
    규칙:
      - 텍스트 내 'Value:'가 보이면 정가=Value, 현재가=첫 번째 금액
      - 금액 2개 이상: 현재가=min, 정가=max
      - 금액 1개: 현재가=정가
    """
    raw = _strip_noise(price_block_text)

    vm = VALUE_RE.search(raw)
    value_price = _to_float(vm.group(1)) if vm else None
    if value_price is not None and not (0.5 <= value_price <= 500):
        value_price = None  # 말이 안 되는 값은 무시

    amounts = _find_amounts(raw)

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

    discount = None
    if price_current is not None and price_original and price_original > 0:
        d = round((1 - (price_current / price_original)) * 100, 2)
        discount = max(d, 0.0)

    return {
        "price_current_usd": price_current,
        "price_original_usd": price_original if price_original is not None else price_current,
        "discount_rate_pct": discount,
        "value_price_usd": value_price,
        "has_value_price": value_price is not None,
    }
