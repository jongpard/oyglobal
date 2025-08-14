import asyncio
from oy_global import scrape_oliveyoung_global, save_to_csv

async def run():
    print("🔎 올리브영 글로벌몰 베스트 셀러 수집 시작")
    items = await scrape_oliveyoung_global()
    if not items:
        print("⚠️ 수집 결과가 비어있습니다.")
        return
    csv_path = save_to_csv(items)
    # 로그로 미리보기
    print("📁 저장 완료:", csv_path)
    for row in items[:10]:
        print(row["rank"], row["product_name"], row["price_current_usd"], row["price_original_usd"])

if __name__ == "__main__":
    asyncio.run(run())
