# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Tuple

PRICE_RE = re.compile(r"US\$\s*([\d]+(?:\.\d{1,2})?)")

def parse_price(text: str) -> float:
    m = PRICE_RE.search(text or "")
    return float(m.group(1)) if m else 0.0

def pct_round(cur: float, orig: float) -> int:
    if orig <= 0:
        return 0
    from math import isfinite
    if not (isfinite(cur) and isfinite(orig)):
        return 0
    return int(round(max(0.0, 100.0 * (1.0 - cur / orig))))
