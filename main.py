import asyncio
import csv
import os
from datetime import datetime, timezone, timedelta

from oy_global import scrape_oliveyoung_global


KST = timezone(timedelta(hours=9))
DATA_DIR = "data"


def _now_kst_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _save_csv(rows, out_path: str) -> None:
    _ensure_dir(os.path.dirname(out_path))
    headers = [
        "date_kst",
        "rank",
        "brand",
        "product_name",
        "price_current_usd",
        "price_original_usd",
        "discount_rate_pct",
        "value_price_usd",
        "has_value_price",
        "product_url",
        "image_url",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in headers})


def _pretty_title(brand: str, name: str) -> str:
    b = (brand or "").strip()
    n = (name or "").strip()
    if not b:
        return n
    # 제품명이 브랜드로 시작하면 중복 방지
    if n.lower().startswith(b.lower()):
        return n
    return f"{b} {n}"


def _format_top10_for_slack(rows) -> str:
    lines = []
    title = f"올리브영 글로벌 전체 랭킹 ({_now_kst_date()} KST)"
    lines.append(title)
    lines.append("TOP 10")
    for r in rows[:10]:
        name = _pretty_title(r["brand"], r["product_name"])
        url = r["product_url"]
        cur = r.get("price_current_usd")
        orig = r.get("price_original_usd")
        disc = r.get("discount_rate_pct")
        # 슬랙 링크 포맷
        link = f"<{url}|{name}>"
        # 가격 라인
        price_bits = []
        if cur:
            price_bits.append(f"US${cur}")
        if orig:
            price_bits.append(f"(정가 US${orig})")
        if disc is not None and disc != "":
            price_bits.append(f"(↓{disc}%)")
        price_str = " ".join(price_bits) if price_bits else ""
        lines.append(f"{r['rank']}. {link} – {price_str}")
    lines.append("")  # 끝 줄
    return "\n".join(lines)


async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()
    # rank 보정(1~100)
    for idx, it in enumerate(items[:100], start=1):
        it["rank"] = idx
        it["date_kst"] = _now_kst_date()

    out_path = os.path.join(DATA_DIR, f"oliveyoung_global_{_now_kst_date()}.csv")
    _save_csv(items[:100], out_path)
    print(f"📁 저장 완료: {out_path}")

    # 슬랙 알림(옵션)
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if webhook:
        import json, urllib.request

        payload = {"text": _format_top10_for_slack(items)}
        req = urllib.request.Request(
            webhook,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"✅ Sent Slack message. status={resp.status}")
        except Exception as e:
            print(f"⚠️ Slack 전송 실패: {e}")
    else:
        print("ℹ️ SLACK_WEBHOOK_URL 미설정 — 슬랙 전송 생략")


if __name__ == "__main__":
    asyncio.run(run())
