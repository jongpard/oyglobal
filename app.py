# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 수집/비교/알림 (USD)
- HTTP → 실패/부족 시 Playwright(stealth, 지역 강제, XHR 스니핑)
- 저장: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST)
- 전일 CSV 비교(Top30) → Slack 알림
"""
import os, re, io, json, math, pytz, traceback, base64, random
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

# ---------------- 날짜/유틸 ----------------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"올리브영글로벌_랭킹_{d}.csv"

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def to_float(s):
    if s is None: return None
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
def slack_escape(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
def fmt_currency_usd(v): return f"${(v or 0):,.2f}"

def remove_brand_from_title(title: str, brand: str) -> str:
    t, b = clean_text(title), clean_text(brand)
    if not b: return t
    for pat in [rf"^\[?\s*{re.escape(b)}\s*\]?\s*[-–—:|]*\s*", rf"^\(?\s*{re.escape(b)}\s*\)?\s*[-–—:|]*\s*"]:
        t2 = re.sub(pat, "", t, flags=re.I)
        if t2 != t: return t2.strip()
    return t

@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]
    orig_price: Optional[float]
    discount_percent: Optional[int]
    url: str

# ---------------- 파서 ----------------
def parse_cards_from_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    item_selectors = [
        "ul.tab_cont_list li",
        "ul.best_list li",
        "ul#bestSellerContent li",
        "li.prod_item",
        "ul li",
        "div.prod_area",
        "div.product_item",
        "div.item"
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

# ---------------- HTTP 시도 ----------------
def fetch_by_http() -> List[Product]:
    hdrs = {
        # 일부 사이트가 최신 크롬 UA를 요구
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        # 메모: ‘한국에서 접속해야 보인다’ 이슈를 고려해 ko 우선
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=hdrs, timeout=25)
    r.raise_for_status()
    return parse_cards_from_html(r.text)

# ---------------- Playwright 폴백(stealth + 지역/통화 강제 + XHR 스니핑) ----------------
def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright

    def try_once(country_code: str, currency_code: str, accept_lang: str) -> Tuple[List[Product], dict]:
        with sync_playwright() as p:
            # Stealth에 유리한 args
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1366, "height": 900},
                locale=accept_lang.split(",")[0],
                timezone_id="Asia/Seoul" if country_code=="KR" else "America/Los_Angeles",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                extra_http_headers={"Accept-Language": accept_lang},
            )

            # webdriver 흔적 제거
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                  parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters)
                );
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(param){
                  if (param === 37445) return 'Intel Open Source Technology Center';
                  if (param === 37446) return 'Mesa DRI Intel(R) UHD Graphics 620 (Kabylake GT2)';
                  return getParameter.call(this, param);
                };
                try {
                  localStorage.setItem('country', '%s');
                  localStorage.setItem('oy-country', '%s');
                  localStorage.setItem('currency', '%s');
                  localStorage.setItem('oy-currency', '%s');
                  localStorage.setItem('locale', '%s');
                } catch(e){}
            """ % (country_code, country_code, currency_code, currency_code, accept_lang.split(",")[0]))

            # 쿠키도 같이 세팅(없어도 무해)
            try:
                context.add_cookies([
                    {"name":"country", "value":country_code, "domain":"global.oliveyoung.com", "path":"/"},
                    {"name":"currency","value":currency_code,"domain":"global.oliveyoung.com","path":"/"},
                ])
            except: pass

            page = context.new_page()

            # XHR(JSON) 스니핑
            sniff = {"jsons": []}
            def on_response(resp):
                try:
                    ct = resp.headers.get("content-type","")
                    url = resp.url
                    if ("application/json" in ct or url.endswith(".json")) and any(k in url.lower() for k in ["best", "rank", "list"]):
                        sniff["jsons"].append({"url": url, "body": resp.text()})
                except: pass
            page.on("response", on_response)

            page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
            try: page.wait_for_load_state("networkidle", timeout=30_000)
            except: pass

            # 팝업/동의 닫기
            for sel in [
                "#onetrust-accept-btn-handler",
                "button:has-text('Accept All')",
                "button:has-text('Accept')",
                "button:has-text('동의')",
                "button:has-text('확인')",
                "[aria-label='Close']",
            ]:
                try: page.locator(sel).first.click(timeout=1200)
                except: pass

            # 탭/필터 클릭 시도 (있으면)
            for t in ["BEST", "Best", "BEST SELLER", "Best Seller", "All", "전체"]:
                try:
                    page.get_by_text(t, exact=False).first.click(timeout=1200)
                    page.wait_for_timeout(400)
                except: pass

            # 스크롤로 지연로딩 유도
            try:
                for _ in range(10):
                    page.mouse.wheel(0, 2200); page.wait_for_timeout(600)
                page.mouse.wheel(0, -8000); page.wait_for_timeout(600)
            except: pass

            # 1차: DOM 파싱
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
                    'ul.tab_cont_list li','ul.best_list li','ul#bestSellerContent li',
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
            if data and len(data) >= 10:
                for row in data:
                    sale = to_float(row.get("price")); orig = to_float(row.get("orig"))
                    pct  = extract_percent_floor(orig, sale, row.get("pct"))
                    products.append(Product(row.get("rank"), clean_text(row.get("brand")),
                                            clean_text(row.get("title")), sale, orig, pct, row.get("link")))
                html = page.content()
                context.close(); browser.close()
                return products, sniff

            # 2차: HTML 재파싱
            html = page.content()
            products = parse_cards_from_html(html)
            if len(products) >= 10:
                context.close(); browser.close()
                return products, sniff

            # 3차: 스니핑한 JSON에서 후처리(느슨하게)
            for blob in sniff["jsons"]:
                try:
                    j = json.loads(blob["body"])
                except:
                    continue
                # 이름/가격/URL 후보를 넓게 탐색
                flat = json.dumps(j, ensure_ascii=False)
                # 링크 후보
                urls = re.findall(r"https?://global\.oliveyoung\.com[^\s\"']+", flat)
                names = [m for m in re.findall(r"\"(?:name|title)\"\s*:\s*\"([^\"]{3,100})\"", flat)]
                prices = [to_float(x) for x in re.findall(r"\"(?:sale|final|price)[^\"]*\"\s*:\s*([0-9]+(?:\.[0-9]+)?)", flat)]
                # 대략 매칭 (안정성보다 회수 우선)
                candidates = []
                for i, nm in enumerate(names[:60]):  # 과도 방지
                    u = urls[i] if i < len(urls) else (urls[-1] if urls else "")
                    pr = prices[i] if i < len(prices) else None
                    candidates.append(Product(None, "", nm, pr, None, None, u))
                if len(candidates) >= 10:
                    # 순서대로 랭크 부여
                    for idx, c in enumerate(candidates, start=1): c.rank = idx
                    context.close(); browser.close()
                    return candidates, sniff

            # 디버그 저장
            png = page.screenshot()
            context.close(); browser.close()
            return [], {"jsons": sniff["jsons"], "html": html, "screenshot": base64.b64encode(png).decode()}
    # KR 먼저(이전 대화 기준), 실패 시 US
    items, dbg = try_once("KR", "USD", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    if len(items) >= 10: return items
    items, dbg2 = try_once("US", "USD", "en-US,en;q=0.9,ko-KR;q=0.6")
    # 디버그 저장
    ensure_dir("data/debug")
    try:
        if isinstance(dbg, dict):
            if "html" in dbg and dbg["html"]:
                with open("data/debug/page_kr.html","w",encoding="utf-8") as f: f.write(dbg["html"])
            if "screenshot" in dbg and dbg["screenshot"]:
                with open("data/debug/page_kr.png","wb") as f: f.write(base64.b64decode(dbg["screenshot"]))
            for i, js in enumerate(dbg.get("jsons", [])[:5], 1):
                with open(f"data/debug/json_kr_{i}.json","w",encoding="utf-8") as f: f.write(js.get("body",""))
    except: pass
    try:
        if isinstance(dbg2, dict):
            if "html" in dbg2 and dbg2["html"]:
                with open("data/debug/page_us.html","w",encoding="utf-8") as f: f.write(dbg2["html"])
            if "screenshot" in dbg2 and dbg2["screenshot"]:
                with open("data/debug/page_us.png","wb") as f: f.write(base64.b64decode(dbg2["screenshot"]))
            for i, js in enumerate(dbg2.get("jsons", [])[:5], 1):
                with open(f"data/debug/json_us_{i}.json","w",encoding="utf-8") as f: f.write(js.get("body",""))
    except: pass
    return items  # 빈 리스트일 수 있음

def fetch_products() -> List[Product]:
    # 1) HTTP
    try:
        items = fetch_by_http()
        if len(items) >= 10:
            return items
    except Exception as e:
        print("[HTTP] 실패/부족:", e)
    # 2) Playwright
    return fetch_by_playwright()

# ---------------- Drive / Slack ----------------
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
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------------- 비교/메시지 ----------------
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

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)]
    if len(t30)==0:
        return S

    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)]
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
    parts += ["", "*📉 급하락*"] + (S["falling"] or ["- 해당 없음"])
    for line in S.get("outs", []): parts.append(line)
    parts += ["", "*🔄 랭크 인&아웃*", f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다."]
    return "\n".join(parts)

# ---------------- 메인 ----------------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    items = fetch_products()
    print("수집 완료:", len(items))

    ensure_dir("data")
    if len(items) < 10:
        # 실패 상황도 CSV는 비워서 남겨두고, Slack 알림은 보내되 종료코드는 0으로(스케줄 계속)
        pd.DataFrame([], columns=["date","rank","brand","product_name","price","orig_price","discount_percent","url","otuk"])\
          .to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
        slack_post("*올리브영 글로벌몰 랭킹 — 수집 실패*\n- 원인: 렌더링/지역/봇감지 이슈 가능성\n- `data/debug/` 아티팩트 확인 필요")
        print("[경고] 10개 미만 → 디버그 저장됨(data/debug)."); return

    df_today = to_dataframe(items, date_str)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    df_prev = None
    folder = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견")
        except Exception as e:
            print("Drive 오류:", e); traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try: slack_post(f"*올리브영 글로벌몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
