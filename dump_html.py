# dump_html.py
import asyncio, os, time
from playwright.async_api import async_playwright

URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
OUT_DIR = "data/debug"

async def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = await browser.new_context(
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            extra_http_headers={"Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8"},
            viewport={"width":1280,"height":900},
        )
        page = await ctx.new_page()
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        # 배송지 대한민국 선택 시도(가능한 경우)
        for sel in ['text=배송지','text=Ship to','button:has-text("배송지")','a:has-text("Ship to")']:
            try:
                await page.locator(sel).first.click(timeout=1000); break
            except: pass
        for sel in ['text=대한민국','text=Korea']:
            try:
                await page.locator(sel).first.click(timeout=1000); break
            except: pass
        for sel in ['text=저장','text=확인','text=Save','text=Apply']:
            try:
                await page.locator(sel).first.click(timeout=1000); break
            except: pass

        # 살짝 스크롤해서 목록 로드
        for _ in range(10):
            await page.evaluate("window.scrollBy(0, 1600)"); await page.wait_for_timeout(350)

        html = await page.content()
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUT_DIR, f"oy_best_snapshot_{ts}.html")
        with open(out_path, "w", encoding="utf-8") as f: f.write(html)
        print("[saved]", out_path)
        await ctx.close(); await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
