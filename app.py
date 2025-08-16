# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 자동화 (USD)
- 소스: https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1
- HTTP(정적) → 부족 시 Playwright(동적) 폴백
- 파일명: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST)
- 전일 CSV 비교 Top30 → Slack 알림
환경변수:
  SLACK_WEBHOOK_URL
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN   # OAuth 권장
  GDRIVE_SERVICE_ACCOUNT_JSON                                    # 선택(Shared Drive 등)
  GDRIVE_FOLDER_ID
  DRIVE_AUTH_MODE=oauth|oauth_only|service_account               # 기본 oauth_only 권장
"""
import os, re, io, math, json, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------- 기본설정/유틸 ----------------
BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"올리브영글로벌_랭킹_{d}.csv"

def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def to_float(s):
    if not s: return None
    m = re.findall(r"[\d]+(?:\.[\d]+)?", str(s))
    return float(m[0]) if m else None

def parse_price_to_float(text: str) -> Optional[float]:
    if not text: return None
    t = text.replace("US$", "").replace("$", "").replace(",", "").strip()
    try: return float(t)
    except: return None

def fmt_currency_usd(v) -> str:
    try:
        if v is None: return "$0.00"
        if isinstance(v, float) and math.isnan(v): return "$0.00"
        return f"${float(v):,.2f}"
    except:
        return "$0.00"

def slack_escape(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def make_display_name(brand: str, product: str, include_brand: bool) -> str:
    """브랜드+제품명(중복 방지). include_brand=False면 제품명만."""
    product = clean_text(product); brand = clean_text(brand)
    if not include_brand or not brand: return product
    if re.match(rf"^\[?\s*{re.escape(brand)}\b", product, flags=re.I): return product
    return f"{brand} {product}"

def discount_floor(orig: Optional[float], sale: Optional[float], percent_text: Optional[str]) -> Optional[int]:
    if percent_text:
        n = to_float(percent_text)
        if n is not None: return int(n // 1)
    if orig and sale and orig > 0:
        return max(0, int(math.floor((1 - sale / orig) * 100)))
    return None

@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]
    orig_price: Optional[float]
    discount_percent: Optional[int]
    url: str

# ---------------- 정적 파서(스켈레톤이면 0개) ----------------
def parse_static_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("#orderBestProduct > li.order-best-product")
    items: List[Product] = []
    for idx, li in enumerate(cards, start=1):
        # 제품명: hidden input[name='prdtName']
        name = ""
        inp = li.select_one("input[name='prdtName']")
        if inp and inp.has_attr("value"): name = clean_text(inp["value"])
        if not name:
            nm = li.select_one(".product_name, .name, .tit, .item_name")
            if nm: name = clean_text(nm.get_text(" ", strip=True))

        brand = ""
        b = li.select_one("dl.brand-info dt, .brand, .brand_name, .brandName")
        if b: brand = clean_text(b.get_text(" ", strip=True))

        link = ""
        a = li.select_one("a")
        if a and a.has_attr("href"): link = a["href"]
        if link.startswith("/"): link = "https://global.oliveyoung.com" + link

        rank = None
        span = li.select_one(".rank-badge span, .rank_num")
        if span:
            rnum = to_float(clean_text(span.get_text()))
            if rnum is not None: rank = int(rnum)
        if rank is None: rank = idx

        pbox = li.select_one(".price-info") or li
        ptxt = clean_text(pbox.get_text(" ", strip=True))
        amts = [parse_price_to_float(m) for m in re.findall(r"(?:US\$|\$)\s*([\d.,]+)", ptxt)]
        amts = [a for a in amts if a is not None]
        sale = orig = None
        if len(amts) == 1: sale = amts[0]
        elif len(amts) >= 2: sale, orig = min(amts), max(amts)

        sale_txt = li.select_one(".price-info strong.point")
        orig_txt = li.select_one(".price-info span")
        sale = sale or (parse_price_to_float(sale_txt.get_text()) if sale_txt else None)
        orig = orig or (parse_price_to_float(orig_txt.get_text()) if orig_txt else None)

        pct_txt = ""
        pct_node = li.select_one(".price-info .rate, .discount-rate, .percent, .dc")
        if pct_node: pct_txt = clean_text(pct_node.get_text())

        pct = discount_floor(orig, sale, pct_txt)
        if name and link:
            items.append(Product(rank, brand, name, sale, orig, pct, link))
    return items

# ---------------- HTTP → Playwright ----------------
def fetch_by_http() -> List[Product]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=headers, timeout=25)
    r.raise_for_status()
    return parse_static_html(r.text)

def fetch_by_playwright() -> List[Product]:
    """동적 DOM 파싱: 카드=#orderBestProduct > li.order-best-product"""
    from playwright.sync_api import sync_playwright
    CARD_SEL = "#orderBestProduct > li.order-best-product"
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            viewport={"width":1366,"height":900},
            locale="ko-KR", timezone_id="Asia/Seoul",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        context.add_init_script("""try{localStorage.setItem('country','KR');localStorage.setItem('currency','USD');}catch(e){}""")

        page = context.new_page()
        page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass
        for sel in ["#onetrust-accept-btn-handler","button:has-text('Accept')","button:has-text('확인')","[aria-label='Close']"]:
            try: page.locator(sel).first.click(timeout=1200)
            except: pass
        for _ in range(8):
            try: page.mouse.wheel(0,2200); page.wait_for_timeout(600)
            except: break
        page.wait_for_selector(CARD_SEL, timeout=60_000)

        data = page.evaluate("""
            (SEL) => {
              const nodes = Array.from(document.querySelectorAll(SEL));
              const get = (el, s) => (el.querySelector(s)?.textContent || '').replace(/\\s+/g,' ').trim();
              const amt = t => { if(!t) return null; const v=parseFloat(t.replace(/US\\$|\\$|,/g,'').trim()); return isNaN(v)?null:v; };
              return nodes.map((el, idx) => {
                const name  = (el.querySelector("input[name='prdtName']")?.value || '').trim();
                const brand = get(el, "dl.brand-info dt, .brand, .brand_name, .brandName");
                const link  = el.querySelector("a")?.href || '';
                const rtxt  = get(el, ".rank-badge span, .rank_num");
                const rank  = parseInt(rtxt) || (idx+1);
                let sale = amt(get(el, ".price-info strong.point"));
                let orig = amt(get(el, ".price-info span"));
                if (sale == null && orig != null) sale = orig;   // 가격 누락 보정
                const pctTxt = get(el, ".price-info .rate, .discount-rate, .percent, .dc");
                return {rank, brand, name, link, sale, orig, pctTxt};
              }).filter(x => x.name && x.link);
            }
        """, CARD_SEL)
        context.close(); browser.close()

    items: List[Product] = []
    for r in data:
        items.append(Product(
            rank=int(r["rank"]), brand=clean_text(r["brand"]), title=clean_text(r["name"]),
            price=r["sale"], orig_price=r["orig"],
            discount_percent=discount_floor(r["orig"], r["sale"], r["pctTxt"]), url=r["link"]
        ))
    return items

def fetch_products() -> List[Product]:
    try:
        items = fetch_by_http()
        if len(items) >= 10: return items
    except Exception as e:
        print("[HTTP 오류] → Playwright 폴백:", e)
    return fetch_by_playwright()

# ---------------- Google Drive ----------------
def build_drive_service(mode: Optional[str] = None):
    """
    mode:
      - "oauth" (기본): OAuth 우선, 없으면 SA
      - "oauth_only": OAuth만, 없으면 에러
      - "service_account": SA만, 없으면 에러
    """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    from google.oauth2 import service_account as gsa

    scopes = ["https://www.googleapis.com/auth/drive"]
    if mode is None:
        mode = os.getenv("DRIVE_AUTH_MODE", "oauth_only").lower()

    cid  = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
    sa_json = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()

    def make_oauth():
        if not (cid and csec and rtk): return None
        return Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                           client_id=cid, client_secret=csec, scopes=scopes)

    def make_sa():
        if not sa_json: return None
        info = json.loads(sa_json)
        return gsa.Credentials.from_service_account_info(info, scopes=scopes)

    creds = None
    used = "unknown"

    if mode in ("oauth", "oauth_only"):
        creds = make_oauth()
        if creds:
            used = "oauth"
        elif mode == "oauth_only":
            raise RuntimeError("OAuth 자격정보가 없습니다. GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN 확인")

    if creds is None and mode in ("oauth", "service_account"):
        sa_creds = make_sa()
        if sa_creds:
            creds = sa_creds
            used = "service_account"

    if creds is None: raise RuntimeError("Google Drive 자격정보가 없습니다.")
    print(f"[Drive] auth={used}")
    return build("drive", "v3", credentials=creds, cache_discovery=False), (used == "service_account")

def drive_preflight(service, folder_id: str) -> bool:
    """폴더 접근 권한/존재 확인."""
    from googleapiclient.errors import HttpError
    try:
        meta = service.files().get(
            fileId=folder_id,
            fields="id,name,driveId",
            supportsAllDrives=True,
        ).execute()
        print(f"[Drive] folder OK: name='{meta.get('name')}', driveId='{meta.get('driveId','MyDrive')}'")
        return True
    except HttpError as e:
        print("[Drive] folder access error:", e)
        return False

def drive_upload_csv(service, is_sa: bool, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    from googleapiclient.errors import HttpError

    def _do_upload(svc):
        q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
        res = svc.files().list(
            q=q, fields="files(id,name,driveId)",
            supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

        buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
        media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
        if file_id:
            svc.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
            return file_id
        meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
        created = svc.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
        return created["id"]

    try:
        return _do_upload(service)
    except HttpError as e:
        msg = f"{e}"
        if is_sa and ("storageQuotaExceeded" in msg or "Service Accounts do not have storage quota" in msg):
            print("[Drive] SA 403(storageQuotaExceeded) → OAuth-only 재시도")
            svc2, _ = build_drive_service("oauth_only")
            if not drive_preflight(svc2, folder_id):
                raise
            return _do_upload(svc2)
        raise

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(
        q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
        fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ---------------- Slack ----------------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------------- 비교/메시지 ----------------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "brand": p.brand,
        "product_name": p.title,
        "price": p.price,
        "orig_price": p.orig_price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "otuk": False if p.rank is not None else True,
    } for p in products])

def line_move(name_link: str, prev_rank: Optional[int], curr_rank: Optional[int]) -> Tuple[str, int]:
    if prev_rank is None and curr_rank is not None: return f"- {name_link} NEW → {curr_rank}위", 99999
    if curr_rank is None and prev_rank is not None: return f"- {name_link} {prev_rank}위 → OUT", 99999
    if prev_rank is None or curr_rank is None:    return f"- {name_link}", 0
    delta = prev_rank - curr_rank
    if   delta > 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↑{delta})", delta
    elif delta < 0: return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↓{abs(delta)})", abs(delta)
    else:           return f"- {name_link} {prev_rank}위 → {curr_rank}위 (변동없음)", 0

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}

    # TOP10 (브랜드 포함)
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        disp = make_display_name(r.get("brand",""), r["product_name"], include_brand=True)
        name_link = f"<{r['url']}|{slack_escape(disp)}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent"); tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    # 전일 CSV 없으면 비교 섹션 스킵
    if df_prev is None or not len(df_prev):
        return S  # rising/newcomers/falling/outs/inout_count=0

    # --- 이하 전일 CSV가 있을 때만 ---
    df_t = df_today.copy(); df_t["key"] = df_t["url"]; df_t.set_index("key", inplace=True)
    df_p = df_prev.copy(); df_p["key"] = df_p["url"]; df_p.set_index("key", inplace=True)

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()
    common = set(t30.index) & set(p30.index)
    new    = set(t30.index) - set(p30.index)
    out    = set(p30.index) - set(t30.index)

    def full_name_link(row):
        disp = make_display_name(row.get("brand",""), row.get("product_name",""), include_brand=True)
        return f"<{row['url']}|{slack_escape(disp)}>"

    # 🔥 급상승
    rising = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            rising.append((imp, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [e[-1] for e in rising[:3]]

    # 🆕 뉴랭커
    newcomers = []
    for k in new:
        curr_rank = int(t30.loc[k,"rank"])
        newcomers.append((curr_rank, f"- {full_name_link(t30.loc[k])} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [line for _, line in newcomers[:3]]

    # 📉 급하락
    falling = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            falling.append((drop, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [e[-1] for e in falling[:5]]

    # OUT
    for k in sorted(list(out)):
        prev_rank = int(p30.loc[k,"rank"])
        line,_ = line_move(full_name_link(p30.loc[k]), prev_rank, None)
        S["outs"].append(line)

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    parts = [f"*올리브영 글로벌몰 랭킹 — {date_str}*", "",
             "*TOP 10*"] + (S["top10"] or ["- 데이터 없음"]) + ["",
             "*🔥 급상승*"] + (S["rising"] or ["- 해당 없음"]) + ["",
             "*🆕 뉴랭커*"] + (S["newcomers"] or ["- 해당 없음"]) + ["",
             "*📉 급하락*"] + (S["falling"] or ["- 해당 없음"])]
    # 급하락 섹션에 OUT도 함께 표기
    parts += S.get("outs", [])
    parts += ["", "*🔄 랭크 인&아웃*", f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다."]
    return "\n".join(parts)

# ---------------- 메인 ----------------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    items = []
    try:
        items = fetch_by_http()
        print(f"[HTTP] 수집: {len(items)}개")
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
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    df_prev = None
    folder = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if folder:
        try:
            prefer = os.getenv("DRIVE_AUTH_MODE","oauth_only").lower()
            svc, is_sa = build_drive_service(prefer)

            # 폴더 접근 프리체크
            if not drive_preflight(svc, folder):
                if is_sa:
                    print("[Drive] SA → OAuth-only 재시도")
                    svc, is_sa = build_drive_service("oauth_only")
                    if not drive_preflight(svc, folder):
                        raise RuntimeError("Google Drive 폴더 접근 불가: FOLDER_ID/권한 확인 필요")
                else:
                    raise RuntimeError("Google Drive 폴더 접근 불가: FOLDER_ID/권한 확인 필요")

            drive_upload_csv(svc, is_sa, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견")
        except Exception as e:
            print("Google Drive 처리 오류:", e); traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try: slack_post(f"*올리브영 글로벌몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
