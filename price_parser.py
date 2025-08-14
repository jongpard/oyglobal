# price_parser.py
import re
from typing import Optional

_PRICE_RE = re.compile(r"([\d]+(?:[.,]\d{1,2})?)")

def parse_price(text: Optional[str]) -> Optional[float]:
    """'US$25.99' 같은 문자열에서 숫자만 안전하게 float로 파싱."""
    if not text:
        return None
    text = text.replace(",", "")
    m = _PRICE_RE.search(text)
    return float(m.group(1)) if m else None
