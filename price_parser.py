import re
from typing import Optional

_MONEY_RE = re.compile(r"(?:USD|\$)\s*([0-9]{1,3}(?:[,][0-9]{3})*(?:\.[0-9]{1,2})?|[0-9]+(?:\.[0-9]{1,2})?)")

def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = _MONEY_RE.search(text.replace("\u00A0", " "))
    if not m:
        # $ 없이 "12.34" 같은 케이스도 허용
        try:
            val = float(text.strip().replace(",", ""))
            return val
        except Exception:
            return None
    val = m.group(1).replace(",", "")
    try:
        return float(val)
    except Exception:
        return None
