# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 수집/비교/알림 (USD)
- 데이터 소스: https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1
- HTTP → 실패/부족 시 Playwright
- 파일명: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST)
- 전일 CSV 비교(Top30 기준) → Slack 알림
"""
import os, re, io, math, json, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

# ---------- 공통 유틸 ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"올리브영글로벌_랭킹_{d}.csv"

def to_float(s):
    if not s: return None
    m = re.findall(r"[\d]+(?:\.[\d]+)?", str(s))
    return float(m[0]) if m else None

def extract_percent_floor(orig_price, sale_price, percent_text):
    if percent_text:
        n = to_float(percent_text)
        if n is not None: return int(n // 1)
    if orig_price and sale_price and orig_price > 0:
        pct = (1 - (sale_price / orig_price)) * 100.0
        return max(0, int(pct // 1))
    return None

def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()

def remove_brand_from_title(title, brand):
    t, b = clean_text(title), clean_text(brand)
    if not b: return t
    for pat in [
        rf"^\[?\s*{re.escape(b)}\s*\]?\s*[-–—:|]*\s*",
        rf"^\(?\s*{re.escape(b)}\s*\)?\s*[-–—:|]*\s*",
    ]:
        t2 = re.sub(pat, "", t, flags=re.I)
        if t2 != t: return t2.strip()
    return t

def slack_escape(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
def fmt_currency_usd(v): return f"${(v or 0):,.2f}"

@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]
    orig_price: Optional[float]
    discount_percent: Optional[int]
    url: str

# ---------- HTML 파서 ----------
def parse_cards_from_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    item_selectors = [
        "ul.tab_cont_list li",
        "ul.best_list li",
        "ul#bestSellerContent li",
        "li.prod_item",
        "ul li",
        "div.prod_area",
        "div.product_item, div.item"  # 여유 셀렉터
    ]
    name_selectors = [".product_name", ".prod_name", ".name", ".tit", ".tx_name", ".item_name", "a[title]"]
    brand_selectors = [".brand", ".brand_name", ".tx_brand", ".brandName"]
    link_selectors  = ["a.prod_link", "a.link", "a.detail_link", "a"]
    price_selectors = [".price .num", ".sale_price", ".discount_price", ".final_price", ".price", ".value"]
    orig_price_selectors = [".orig_price", ".normal_price", ".consumer", ".strike", ".was"]
    percent_selectors = [".percent", ".dc", ".discount_rate", ".rate"]

    def pick_text(el, sels):
        for sel in sels:
            node = el.select_one(sel)
            if node:
                t = clean_text(node.get_text(" ", strip=True))
                if t: return t
        return ""

    def pick_link(el, sels):
        for sel in sels:
            a = el.select_one(sel)
            if a and a.has_attr("href"):
                href = a["href"].strip()
                if href and not href.startswith("javascript"): return href
        return ""

    found = []
    for sel in item_selectors:
        found = soup.select(sel)
        if len(found) >= 10: break

    items: List[Product] = []
    for idx, li in enumerate(found, start=1):
        title = pick_text(li, name_selectors)
        brand = pick_text(li, brand_selectors)
        link  = pick_link(li, link_selectors)
        sale  = to_float(pick_text(li, price_selectors))
        orig  = to_float(pick_text(li, orig_price_selectors))
        pct   = extract_percent_floor(orig, sale, pick_text(li, percent_selectors))

        if link and link.startswith("/"):
            link = "https://global.oliveyoung.com" + link
        if title and link:
            items.append(Product(idx, brand, title, sale, orig, pct, link))
    return items

# ---------- 1) HTTP 시도 ----------
def fetch_by_http() -> List[Product]:
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=hdrs, timeout=25)
    r.raise_for_status()
    return parse_cards_from_html(r.text)

# ---------- 2) Playwright 폴백 (강화) ----------
def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = p.chromium.launch_persistent_context(
            user_data_dir="/tmp/oyg",
            headless=True,
            locale="en-US",
            timezone_id="America/Los_Angeles",
            geolocation={"latitude": 37.7749, "longitude": -122.4194},  # US
            permissions=["geolocation"],
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

        page = context.pages[0] if context.pages else context.new_page()

        # 지역/통화 강제(추정 키 포함) — 존재하지 않아도 무해
        page.add_init_script("""
            try {
              localStorage.setItem('country', 'US');
              localStorage.setItem('oy-country', 'US');
              localStorage.setItem('currency', 'USD');
              localStorage.setItem('oy-currency', 'USD');
              localStorage.setItem('locale', 'en-US');
            } catch(e){}
        """)
        # 쿠키도 미리 세팅(있으면 사용)
        try:
            context.add_cookies([
                {"name": "country", "value": "US", "domain": "global.oliveyoung.com", "path": "/"},
                {"name": "currency", "value": "USD", "domain": "global.oliveyoung.com", "path": "/"},
            ])
        except: pass

        page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass

        # 쿠키/동의/팝업 닫기
        for sel in [
            "button#onetrust-accept-btn-handler",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            "button:has-text('동의')",
            "button:has-text('확인')",
            "[aria-label='Close']",
            "button:has-text('Don’t open this window')",
        ]:
            try: page.locator(sel).first.click(timeout=1500)
            except: pass

        # 탭 강제: Global / Top Orders / All / USA
        for txt in ["Global", "Top Orders", "All", "USA", "United States"]:
            try:
                page.get_by_text(txt, exact=False).first.click(timeout=1500)
                page.wait_for_timeout(500)
            except: pass

        # 지연 로딩 유도
        try:
            for _ in range(10):
                page.mouse.wheel(0, 2200)
                page.wait_for_timeout(700)
            page.mouse.wheel(0, -8000)
            page.wait_for_timeout(700)
        except: pass

        # 1차: 브라우저 DOM에서 긁기
        data = page.evaluate("""
            () => {
              const pick = (el, sels) => {
                for (const s of sels) {
                  const x = el.querySelector(s);
                  if (x) { const t=(x.textContent||'').replace(/\\s+/g,' ').trim(); if(t) return t; }
                }
                return '';
              };
              const pickLink = (el, sels) => {
                for (const s of sels) {
                  const a = el.querySelector(s);
                  if (a && a.href && !a.href.startsWith('javascript')) return a.href;
                }
                return '';
              };
              const itemSelectors = [
                'ul.tab_cont_list li','ul#bestSellerContent li','ul.best_list li',
                'li.prod_item','ul li','div.prod_area','div.product_item','div.item'
              ];
              const nameSelectors = ['.product_name','.prod_name','.name','.tit','.tx_name','.item_name','a[title]'];
              const brandSelectors = ['.brand','.brand_name','.tx_brand','.brandName'];
              const linkSelectors  = ['a.prod_link','a.link','a.detail_link','a'];
              const priceSelectors = ['.price .num','.sale_price','.discount_price','.final_price','.price','.value'];
              const origSelectors  = ['.orig_price','.normal_price','.consumer','.strike','.was'];
              const percentSelectors = ['.percent','.dc','.discount_rate','.rate'];

              let nodes = [];
              for (const s of itemSelectors) {
                const found = Array.from(document.querySelectorAll(s));
                if (found.length >= 10) { nodes = found; break; }
                if (!nodes.length && found.length) nodes = found;
              }
              return nodes.map((el, idx) => {
                const title = pick(el, nameSelectors);
                const brand = pick(el, brandSelectors);
                const link  = pickLink(el, linkSelectors);
                const price = pick(el, priceSelectors);
                const orig  = pick(el, origSelectors);
                const pct   = pick(el, percentSelectors);
                return {rank: idx+1, title, brand, link, price, orig, pct};
              }).filter(x => x.title && x.link);
            }
        """)

        products: List[Product] = []
        if not data or len(data) < 10:
            # 2차: HTML 통째로 받아 BeautifulSoup 재파싱
            html = page.content()
            products = parse_cards_from_html(html)

        if (not products) and data:
            # DOM 데이터로 구성
            for row in data:
                sale = to_float(row.get("price"))
                orig = to_float(row.get("orig"))
                pct  = extract_percent_floor(orig, sale, row.get("pct"))
                products.append(Product(
                    rank=row.get("rank"),
                    brand=clean_text(row.get("brand")),
                    title=clean_text(row.get("title")),
                    price=sale, orig_price=orig, discount_percent=pct,
                    url=row.get("link"),
                ))

        # 3차(마지막): 페이지 전역 JSON 추출 시도 (Next/Nuxt 등)
        if len(products) < 10:
            try:
                j = page.evaluate("() => (window.__NEXT_DATA__ || window.__NUXT__ || window.__APOLLO_STATE__ || null)")
                if j:
                    s = json.dumps(j)
                    # 매우 느슨한 추출 (링크/이름/가격)
                    candidates = re.findall(r'"(name|title)":"(.*?)".*?"(sale|price)\\w*":\\s*("?\\d+(?:\\.\\d+)?")', s)
                    # 이 루트는 사이트 구조 파악 전 임시 백스톱. 충분치 않으면 무시.
            except: 
                pass

        context.close(); browser.close()
        return products

def fetch_products() -> List[Product]:
    # 1) HTTP
    try:
        items = fetch_by_http()
        if len(items) >= 10:
            return items
    except Exception as e:
        print("[HTTP] 실패/부족 → Playwright:", e)
    # 2) Playwright
    return fetch_by_playwright()

# ---------- Drive / Slack / 비교 로직 (동일) ----------
from googleapiclient.discovery import build
def build_drive_service():
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials
    sa_json = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    scopes = ["https://www.googleapis.com/auth/drive"]
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        cid = os.getenv("GOOGLE_CLIENT_ID")
        csec = os.getenv("GOOGLE_CLIENT_SECRET")
        rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
        if not (cid and csec and rtk):
            raise RuntimeError("Google Drive 자격정보가 없습니다.")
        creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                            client_id=cid, client_secret=csec, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_csv(service, folder_id, name, df):
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)").execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute(); return file_id
    else:
        meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
        return service.files().create(body=meta, media_body=media, fields="id").execute()["id"]

def drive_download_csv(service, folder_id, name):
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)").execute()
    files = res.get("files", [])
    if not files: return None
    file_id = files[0]["id"]
    req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload
    dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

def slack_post(text):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력")
        print(text); return
    r = requests.post(url, json={"text": text}, timeout=15)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str, "rank": p.rank, "brand": p.brand, "product_name": p.title,
        "price": p.price, "orig_price": p.orig_price, "discount_percent": p.discount_percent,
        "url": p.url, "otuk": False if p.rank is not None else True
    } for p in products])

def line_move(name_link, prev_rank, curr_rank):
    if prev_rank is None and curr_rank is not None: return f"- {name_link} NEW → {curr_rank}위", 99999
    if curr_rank is None and prev_rank is not None: return f"- {name_link} {prev_rank}위 → OUT", 99999
    if prev_rank is None or curr_rank is None:    return f"- {name_link}", 0
    delta = prev_rank - curr_rank
    if   delta > 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↑{delta})", delta
    elif delta < 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↓{abs(delta)})", abs(delta)
    else:           return f"- {name_link} {prev_rank}위 → {curr_rank}위 (변동없음)", 0

def build_sections(df_today, df_prev):
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}
    df_t = df_today.copy(); df_t["key"] = df_t["url"]; df_t.set_index("key", inplace=True)
    if df_prev is not None and len(df_prev):
        df_p = df_prev.copy(); df_p["key"] = df_p["url"]; df_p.set_index("key", inplace=True)
    else:
        df_p = pd.DataFrame(columns=df_t.columns)

    top10 = df_t.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        name_only = remove_brand_from_title(r["product_name"], r.get("brand",""))
        name_link = f"<{r['url']}|{slack_escape(name_only)}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent"); tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()
    common = set(t30.index) & set(p30.index)
    new    = set(t30.index) - set(p30.index)
    out    = set(p30.index) - set(t30.index)

    rising = []
    for k in common:
        prev_rank, curr_rank = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            nm = remove_brand_from_title(t30.loc[k,"product_name"], t30.loc[k].get("brand",""))
            link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
            line,_ = line_move(link, prev_rank, curr_rank)
            rising.append((imp, curr_rank, prev_rank, slack_escape(nm), line))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [e[-1] for e in rising[:3]]

    newcomers = []
    for k in new:
        curr_rank = int(t30.loc[k,"rank"])
        nm = remove_brand_from_title(t30.loc[k,"product_name"], t30.loc[k].get("brand",""))
        link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
        newcomers.append((curr_rank, f"- {link} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [line for _, line in newcomers[:3]]

    falling = []
    for k in common:
        prev_rank, curr_rank = int(p30.loc[k,"rank"]), int(t30.loc[k,"rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            nm = remove_brand_from_title(t30.loc[k,"product_name"], t30.loc[k].get("brand",""))
            link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
            line,_ = line_move(link, prev_rank, curr_rank)
            falling.append((drop, curr_rank, prev_rank, slack_escape(nm), line))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [e[-1] for e in falling[:5]]

    for k in sorted(list(out)):
        prev_rank = int(p30.loc[k,"rank"])
        nm = remove_brand_from_title(p30.loc[k,"product_name"], p30.loc[k].get("brand",""))
        link = f"<{p30.loc[k,'url']}|{slack_escape(nm)}>"
        line,_ = line_move(link, prev_rank, None)
        S["outs"].append(line)

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str, S):
    parts = [f"*올리브영 글로벌몰 랭킹 — {date_str}*", "", "*TOP 10*"]
    parts += S["top10"] or ["- 데이터 없음"]
    parts += ["", "*🔥 급상승*"] + (S["rising"] or ["- 해당 없음"])
    parts += ["", "*🆕 뉴랭커*"] + (S["newcomers"] or ["- 해당 없음"])
    parts += ["", "*📉 급하락*"] + (S["falling"] or ["- 해당 없음"]) + S.get("outs", [])
    parts += ["", "*🔄 랭크 인&아웃*", f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다."]
    return "\n".join(parts)

def slack_post_or_print(msg):
    try: slack_post(msg)
    except Exception as e: print("[Slack 오류]", e); print(msg)

def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    items = []
    try:
        items = fetch_by_http()
        print("[HTTP] 수집:", len(items))
    except Exception as e:
        print("[HTTP 오류]", e)

    if len(items) < 10:
        print("[Playwright 폴백 진입]")
        items = fetch_by_playwright()
    print("수집 완료:", len(items))

    if len(items) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    local_path = os.path.join("data", file_today)
    df_today.to_csv(local_path, index=False, encoding="utf-8-sig")
    print("로컬 저장:", local_path)

    drive_folder = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    df_prev = None
    if drive_folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, drive_folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, drive_folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견", file_yesterday)
        except Exception as e:
            print("Google Drive 처리 중 오류:", e); traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post_or_print(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try: slack_post(f"*올리브영 글로벌몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
