# slack_notify.py (메시지 생성 전용)
import math

def _fmt_money(x):
    try:
        return f"US${float(x):.2f}"
    except Exception:
        return "US$0.00"

def build_top10_message(items, date_kst):
    """
    items: scrape 결과(dict) 리스트
    date_kst: 'YYYY-MM-DD'
    - 제품명이 이미 브랜드로 시작하면 브랜드를 중복 표기하지 않음
    - 슬랙 링크 형식: <url|text>
    """
    header = f"*올리브영 글로벌 전체 랭킹* ({date_kst} KST)\n\n*TOP 10*"
    lines = []

    for idx, it in enumerate(items[:10], start=1):
        brand = (it.get("brand") or "").strip()
        name  = (it.get("product_name") or "").strip()
        cur   = it.get("price_current_usd")
        orig  = it.get("price_original_usd")
        pct   = it.get("discount_rate_pct")
        url   = it.get("product_url") or ""

        # 표시명: 제품명이 이미 브랜드로 시작하면 브랜드를 붙이지 않음
        if brand and name.lower().startswith(brand.lower()):
            display = name
        else:
            display = (brand + " " + name).strip() if (brand or name) else "(No name)"

        price_part = f" – {_fmt_money(cur)}"
        extra = []

        if orig and not (isinstance(orig, float) and math.isnan(orig)):
            extra.append(f"(정가 {_fmt_money(orig)})")
        if pct and not (isinstance(pct, float) and math.isnan(pct)):
            arrow = "↓" if pct > 0 else "→"
            extra.append(f"({arrow}{abs(float(pct)):.2f}%)")

        tail = " " + " ".join(extra) if extra else ""
        if url:
            line = f"{idx}. <{url}|{display}>{price_part}{tail}"
        else:
            line = f"{idx}. {display}{price_part}{tail}"

        lines.append(line)

    return header + "\n" + "\n".join(lines)
