# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 자동화 (USD)
- 소스: https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1
- HTTP(정적) → 부족 시 Playwright(동적) 폴백
- 파일명: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST)
- 전일 CSV 비교 Top30 → Slack 알림

필요 환경변수:
  SLACK_WEBHOOK_URL
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
  GDRIVE_FOLDER_ID
  DRIVE_AUTH_MODE = oauth_only (권장)
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

# ---------- 시간/문자 유틸 ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"올리브영글로벌_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def to_float(s):
    if not s: return None
    m = re.findall(r"[\d]+(?:\.[\d]+)?", str(s)); return float(m[0]) if m else None

# ---------- 가격/표기 유틸 ----------
PRICE_RE = re.compile(r"(?:US\$|\$)\s*([\d.,]+)")
def parse_price_to_float(text: str) -> Optional[float]:
    if not text: return None
    t = text.replace("US$", "").replace("$", "").replace(",", "").strip()
    try: return float(t)
    except: return None

def fmt_currency_usd(v) -> str:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return "$0.00"
        return f"${float(v):,.2f}"
    except: return "$0.00"

def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def make_display_name(brand: str, product: str, include_brand: bool) -> str:
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

# ---------- 데이터 모델 ----------
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str   # 제품명 (사이트 표기)
    price: Optional[float]       # 판매가
    orig_price: Optional[float]  # 정가(없으면 판매가와 동일)
    discount_percent: Optional[int]
    url: str

# ---------- 정적 파싱 ----------
def parse_static_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("#pillsTab1Nav1, [id*='pillsTab1Nav1']")
    root = container or soup
    cards = root.select("ul#orderBestProduct li.order-best-product.prdt-unit")
    items: List[Product] = []
    for idx, li in enumerate(cards, start=1):
        name = ""
        nm = li.select_one("dl.brand-info dd, .brand-info dd, .product_name, .name, .tit, .prd_name")
        if nm: name = clean_text(nm.get_text(" ", strip=True))

        brand = ""
        b = li.select_one("dl.brand-info dt, .brand, .brand_name, .brandName")
        if b: brand = clean_text(b.get_text(" ", strip=True))

        a = li.select_one("a[href]")
        link = a["href"] if (a and a.has_attr("href")) else ""
        if link.startswith("/"): link = "https://global.oliveyoung.com" + link

        rtxt = li.select_one(".rank-badge span, .rank-badge")
        rank = None
        if rtxt:
            n = to_float(clean_text(rtxt.get_text()))
            if n is not None: rank = int(n)
        if rank is None: rank = idx

        # 가격: .price-info 전체에서 모든 달러 금액 추출 → sale=min, orig=max
        pbox = li.select_one(".price-info") or li
        ptxt = clean_text(pbox.get_text(" ", strip=True))
        nums = [parse_price_to_float(x) for x in PRICE_RE.findall(ptxt)]
        nums = [x for x in nums if x is not None]
        sale = orig = None
        if len(nums) == 1: sale = nums[0]
        elif len(nums) >= 2: sale, orig = min(nums), max(nums)

        # 백업 셀렉터
        if sale is None:
            sale_el = li.select_one(".price-info .point, .price-info strong, .price-info .sale_price, .price-info .price")
            if sale_el: sale = parse_price_to_float(sale_el.get_text())
        if orig is None:
            orig_el = li.select_one(".price-info span, .price-info del")
            if orig_el: orig = parse_price_to_float(orig_el.get_text())
        if sale is None and orig is not None:
            sale = orig

        pct_txt = ""
        pct_el = li.select_one(".price-info .rate, .discount-rate, .percent, .dc")
        if pct_el: pct_txt = clean_text(pct_el.get_text())
        pct = discount_floor(orig, sale, pct_txt)

        if name and link:
            items.append(Product(rank, brand, name, sale, orig, pct, link))
    return items

def fetch_by_http() -> List[Product]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=headers, timeout=25)
    r.raise_for_status()
    return parse_static_html(r.text)

# ---------- Playwright(강화): Global 전환 + 폴링 대기 ----------
def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright
    import time, pathlib

    CARD_SELS = [
        "ul#orderBestProduct li.order-best-product.prdt-unit",
        "#pillsTab1Nav1 ul#orderBestProduct li.order-best-product.prdt-unit",
        "#pillsTab1Nav1 li.order-best-product.prdt-unit",
    ]

    def _debug_dump(page, tag="global"):
        pathlib.Path("data/debug").mkdir(parents=True, exist_ok=True)
        with open(f"data/debug/page_{tag}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        page.screenshot(path=f"data/debug/page_{tag}.png", full_page=True)

    def _force_region_global(page):
        # 헤더 우측 지역 드롭다운(select 또는 커스텀)
        try:
            sel = page.locator("select.cntry-select-box").first
            if sel.count():
                sel.select_option(label="Global")
                page.wait_for_timeout(500)
                return
        except: pass
        # 커스텀 셀렉터(USA → Global)
        for open_sel in [
            ".cntry-select-box-wrapper .selected-cntry",
            "button[aria-haspopup='listbox']",
            "button:has-text('USA')",
            "[role='button']:has-text('USA')"
        ]:
            try:
                page.locator(open_sel).first.click(timeout=1200)
                for opt in ["li[role='option']:has-text('Global')", "li:has-text('Global')", "text=Global"]:
                    try:
                        page.locator(opt).first.click(timeout=1200)
                        return
                    except: pass
            except: pass
        # 내부 탭도 Global(Top Orders)로 클릭
        for tab_sel in ["[href*='pillsTab1Nav1']", "#pillsTab1Nav1-tab", "[data-bs-target='#pillsTab1Nav1']",
                        "button:has-text('Top Orders')"]:
            try:
                page.locator(tab_sel).first.click(timeout=1200)
                return
            except: pass

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width":1366,"height":900},
            locale="en-US",
            timezone_id="Asia/Seoul",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"en-US,en;q=0.9"},
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        page = context.new_page()
        page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass

        # 쿠키/배너 닫기
        for sel in ["#onetrust-accept-btn-handler","button:has-text('Accept')","button:has-text('확인')","[aria-label='Close']"]:
            try: page.locator(sel).first.click(timeout=1200)
            except: pass

        _force_region_global(page)

        # 스크롤 + 폴링 (최대 35s)
        start = time.time(); found = 0
        while time.time() - start < 35:
            try: page.mouse.wheel(0, 1400)
            except: pass
            for sel in CARD_SELS:
                try: found = page.eval_on_selector_all(sel, "els => els.length")
                except: found = 0
                if found and found >= 10: break
            if found and found >= 10: break
            page.wait_for_timeout(800)

        if not (found and found >= 10):
            _debug_dump(page, "global_empty")
            context.close(); browser.close()
            return []

        # JS로 필요한 필드만 추출
        data = page.evaluate("""
            (sels) => {
              const get = (el, s) => (el.querySelector(s)?.textContent || '').replace(/\\s+/g,' ').trim();
              const numsFrom = (t) => Array.from((t||'').matchAll(/(?:US\\$|\\$)\\s*([\\d.,]+)/g))
                                           .map(m => parseFloat(m[1].replace(/,/g,'')))
                                           .filter(v=>!isNaN(v));
              let nodes = [];
              for (const s of sels) { nodes = Array.from(document.querySelectorAll(s)); if (nodes.length >= 10) break; }
              return nodes.map((el, i) => {
                const brand = get(el, "dl.brand-info dt, .brand, .brand_name, .brandName");
                const name  = get(el, "dl.brand-info dd, .prd_name, .name, .product_name");
                const a     = el.querySelector("a[href]");
                const link  = a ? a.href : '';
                const rtxt  = get(el, ".rank-badge span, .rank-badge");
                const rank  = parseInt((rtxt||'').replace(/[^0-9]/g,'')) || (i+1);

                const pbox  = el.querySelector(".price-info") || el;
                const ptxt  = (pbox.textContent || '').replace(/\\s+/g,' ').trim();
                const arr   = numsFrom(ptxt);
                let sale=null, orig=null;
                if (arr.length===1){ sale=arr[0]; }
                else if (arr.length>=2){ sale=Math.min(...arr); orig=Math.max(...arr); }

                if (sale==null) sale = parseFloat((get(el, ".price-info .point, .price-info strong, .price-info .sale_price, .price-info .price")||'').replace(/[^\\d.]/g,''))||null;
                if (orig==null) orig = parseFloat((get(el, ".price-info span, .price-info del")||'').replace(/[^\\d.]/g,''))||null;
                if (sale==null && orig!=null) sale = orig;

                const pctTxt = get(el, ".price-info .rate, .discount-rate, .percent, .dc");
                return {rank, brand, name, link, sale, orig, pctTxt};
              }).filter(x => x.name && x.link);
            }
        """, CARD_SELS)

        context.close(); browser.close()

    items: List[Product] = []
    for r in data:
        items.append(Product(
            rank=int(r["rank"]),
            brand=clean_text(r["brand"]),
            title=clean_text(r["name"]),
            price=r["sale"],
            orig_price=r["orig"],
            discount_percent=discount_floor(r["orig"], r["sale"], r["pctTxt"]),
            url=r["link"],
        ))
    return items

def fetch_products() -> List[Product]:
    try:
        items = fetch_by_http()
        if len(items) >= 10: return items
    except Exception as e:
        print("[HTTP 오류] → Playwright 폴백:", e)
    return fetch_by_playwright()

# ---------- Google Drive (국내판 스타일: 단순/안정) ----------
def normalize_folder_id(raw: str) -> str:
    """URL/공백/쿼리스트링이 들어와도 순수 folderId만 추출"""
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    """
    OAuth-only. 스코프 미지정(기존 토큰 스코프 사용)으로 invalid_scope 회피.
    """
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    cid  = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csec and rtk):
        raise RuntimeError("OAuth 자격정보가 없습니다. GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN 확인")

    creds = Credentials(
        None,
        refresh_token=rtk,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
        # scopes=None  # ← 의도적으로 지정하지 않음
    )
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        about = svc.about().get(fields="user(displayName,emailAddress)").execute()
        u = about.get("user", {})
        print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    # 동일 파일명 있으면 업데이트, 없으면 생성
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)",
                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)

    if file_id:
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        return file_id

    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id",
                                     supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
                               fields="files(id,name)", supportsAllDrives=True,
                               includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ---------- Slack ----------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------- 비교/메시지 ----------
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

    # 전일 CSV 없으면 비교 섹션 생략
    if df_prev is None or not len(df_prev):
        return S

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

    # 🔥 급상승 (상위 3)
    rising = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            rising.append((imp, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [e[-1] for e in rising[:3]]

    # 🆕 뉴랭커 (≤3)
    newcomers = []
    for k in new:
        curr_rank = int(t30.loc[k,"rank"])
        newcomers.append((curr_rank, f"- {full_name_link(t30.loc[k])} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [line for _, line in newcomers[:3]]

    # 📉 급하락 (상위 5)
    falling = []
    for k in common:
        prev_rank = int(p30.loc[k,"rank"]); curr_rank = int(t30.loc[k,"rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            line,_ = line_move(full_name_link(t30.loc[k]), prev_rank, curr_rank)
            falling.append((drop, curr_rank, prev_rank, slack_escape(t30.loc[k].get("product_name","")), line))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [e[-1] for e in falling[:5]]

    # OUT (급하락 섹션 아래)
    for k in sorted(list(out)):
        prev_rank = int(p30.loc[k,"rank"])
        line,_ = line_move(full_name_link(p30.loc[k]), prev_rank, None)
        S["outs"].append(line)

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*올리브영 글로벌 전체 랭킹 100 — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*");          lines.extend(S.get("top10") or ["- 데이터 없음"]); lines.append("")
    lines.append("*🔥 급상승*");       lines.extend(S.get("rising") or ["- 해당 없음"]); lines.append("")
    lines.append("*🆕 뉴랭커*");       lines.extend(S.get("newcomers") or ["- 해당 없음"]); lines.append("")
    lines.append("*📉 급하락*");       lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.extend(S.get("outs") or [])
    lines.append(""); lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- 메인 ----------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    items: List[Product] = []
    try:
        items = fetch_by_http(); print(f"[HTTP] 수집: {len(items)}개")
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

    # --- Drive 업로드 + 전일 CSV 로드 (국내판 스타일) ---
    df_prev = None
    raw_folder = os.getenv("GDRIVE_FOLDER_ID", "")
    folder = normalize_folder_id(raw_folder)

    if folder:
        try:
            mask = folder[:4] + "…" + folder[-4:]
            print(f"[Drive] folder_id={mask} (normalized)")
            svc = build_drive_service()

            # 곧바로 업로드/다운로드 (프리플라이트 없음)
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)

            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "성공" if df_prev is not None else "미발견")
        except Exception as e:
            print("Google Drive 처리 오류:", e)
            traceback.print_exc()
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
