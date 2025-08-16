# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 자동화 (USD)
- 데이터 소스: https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1
- HTTP(정적) → 부족 시 Playwright(동적) 폴백
- 저장 파일명: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST 기준)
- 전일 CSV와 비교하여 🔥급상승/🆕뉴랭커/📉급하락(OUT 포함)/🔄인&아웃 계산
- Slack 메시지 포맷: 국내 버전과 동일 (모든 제목/소제목 굵게)
- 환경변수: SLACK_WEBHOOK_URL, GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN, GDRIVE_FOLDER_ID, (선택) GDRIVE_SERVICE_ACCOUNT_JSON
"""
import os, re, io, math, json, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------- 기본 설정 ----------------
BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

def now_kst() -> dt.datetime: return dt.datetime.now(KST)
def today_kst_str() -> str:    return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str() -> str:return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d: str) -> str: return f"올리브영글로벌_랭킹_{d}.csv"

def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def to_float(s: Optional[str]) -> Optional[float]:
    if not s: return None
    m = re.findall(r"[\d]+(?:\.[\d]+)?", str(s))
    if not m: return None
    try: return float(m[0])
    except: return None

def parse_price_to_float(text: str) -> Optional[float]:
    if not text: return None
    t = text.replace("US$", "").replace("$", "").replace(",", "").strip()
    try: return float(t)
    except: return None

def fmt_currency_usd(v: Optional[float]) -> str:
    if v is None: return "$0.00"
    return f"${v:,.2f}"

def slack_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def remove_brand_from_title(title: str, brand: str) -> str:
    t = clean_text(title); b = clean_text(brand)
    if not b: return t
    for pat in [
        rf"^\[?\s*{re.escape(b)}\s*\]?\s*[-–—:|]*\s*",
        rf"^\(?\s*{re.escape(b)}\s*\)?\s*[-–—:|]*\s*",
    ]:
        t2 = re.sub(pat, "", t, flags=re.I)
        if t2 != t: return t2.strip()
    return t

def discount_floor(orig: Optional[float], sale: Optional[float], percent_text: Optional[str]) -> Optional[int]:
    # 1) percent_text가 있으면 먼저 사용(버림)
    if percent_text:
        n = to_float(percent_text)
        if n is not None: return int(n // 1)
    # 2) 가격으로 계산(버림)
    if orig and sale and orig > 0:
        return max(0, int(math.floor((1 - sale / orig) * 100)))
    return None

@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]          # 할인가(USD)
    orig_price: Optional[float]     # 정상가(USD)
    discount_percent: Optional[int] # 소수점 없이 버림
    url: str

# ---------------- 정적 HTML 파서(스켈레톤이면 0개일 수 있음) ----------------
def parse_static_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("#orderBestProduct > li.order-best-product")
    items: List[Product] = []
    for idx, li in enumerate(cards, start=1):
        # 제품명: hidden input[name=prdtName]에 존재
        name = ""
        inp = li.select_one("input[name='prdtName']")
        if inp and inp.has_attr("value"): name = clean_text(inp["value"])
        if not name:
            # 보조
            nm = li.select_one(".product_name, .name, .tit, .item_name")
            if nm: name = clean_text(nm.get_text(" ", strip=True))

        brand = ""
        b = li.select_one("dl.brand-info dt, .brand, .brand_name, .brandName")
        if b: brand = clean_text(b.get_text(" ", strip=True))

        # 링크
        link = ""
        a = li.select_one("a")
        if a and a.has_attr("href"): link = a["href"]
        if link.startswith("/"): link = "https://global.oliveyoung.com" + link

        # 순위
        rank = None
        span = li.select_one(".rank-badge span, .rank_num")
        if span:
            rtxt = clean_text(span.get_text())
            rnum = to_float(rtxt)
            if rnum is not None: rank = int(rnum)
        if rank is None: rank = idx

        # 가격/할인율
        price_box = li.select_one(".price-info") or li
        price_text = clean_text(price_box.get_text(" ", strip=True))
        # price_text에서 $ 금액 모두 추출
        amts = [parse_price_to_float(m) for m in re.findall(r"(?:US\$|\$)\s*([\d.,]+)", price_text)]
        amts = [a for a in amts if a is not None]
        sale = orig = None
        if len(amts) == 1:
            sale = amts[0]
        elif len(amts) >= 2:
            sale = min(amts); orig = max(amts)
        # 개별 요소에서도 시도
        sale_txt = li.select_one(".price-info strong.point")
        orig_txt = li.select_one(".price-info span")
        sale = sale or (parse_price_to_float(sale_txt.get_text()) if sale_txt else None)
        orig = orig or (parse_price_to_float(orig_txt.get_text()) if orig_txt else None)

        pct_txt = ""
        pct_node = li.select_one(".price-info .rate, .discount-rate, .percent, .dc")
        if pct_node: pct_txt = clean_text(pct_node.get_text())

        pct = discount_floor(orig, sale, pct_txt)

        if name and link:
            items.append(Product(rank=rank, brand=brand, title=name,
                                 price=sale, orig_price=orig, discount_percent=pct, url=link))
    return items

# ---------------- HTTP 우선 ----------------
def fetch_by_http() -> List[Product]:
    headers = {
        # 글로벌몰이 CSR이라 정적은 보통 스켈레톤이지만 혹시 몰라 시도
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        # 한국 접속 신호(이전 논의), 그래도 USD가 표시되도록 나중에 price 파싱에서 $만 취함
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=headers, timeout=25)
    r.raise_for_status()
    return parse_static_html(r.text)

# ---------------- Playwright (동적 파서) ----------------
def fetch_by_playwright() -> List[Product]:
    """
    디버그 결과 기반:
      - 카드 셀렉터: #orderBestProduct > li.order-best-product
      - 제품명: input[name='prdtName'] (hidden)
      - 가격: .price-info 텍스트에서 US$ 금액 파싱 (sale, orig)
      - 순위: .rank-badge span (없으면 DOM 순서)
    """
    from playwright.sync_api import sync_playwright

    CARD_SEL = "#orderBestProduct > li.order-best-product"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        # 봇 흔적 최소화
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        # 국가/통화 힌트 (없어도 무해)
        context.add_init_script("""
            try {
              localStorage.setItem('country', 'KR');
              localStorage.setItem('currency', 'USD');
              localStorage.setItem('oy-country', 'KR');
              localStorage.setItem('oy-currency', 'USD');
              localStorage.setItem('locale', 'ko-KR');
            } catch(e){}
        """)

        page = context.new_page()
        page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=30_000)
        except: pass

        # 쿠키/동의 배너 닫기(있으면)
        for sel in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept')",
            "button:has-text('확인')",
            "[aria-label='Close']",
        ]:
            try: page.locator(sel).first.click(timeout=1200)
            except: pass

        # 스크롤(지연 로딩)
        try:
            for _ in range(8):
                page.mouse.wheel(0, 2200); page.wait_for_timeout(600)
        except: pass

        # 카드가 붙을 때까지 대기
        page.wait_for_selector(CARD_SEL, timeout=60_000)

        # 브라우저에서 직접 파싱
        data = page.evaluate(
            """
            (CARD_SEL) => {
              const nodes = Array.from(document.querySelectorAll(CARD_SEL));
              const get = (el, sel, attr) => {
                const x = el.querySelector(sel);
                if (!x) return '';
                return attr ? (x.getAttribute(attr)||'') : (x.textContent||'');
              };
              const priceNums = (text) => {
                if (!text) return [];
                const arr = Array.from(text.matchAll(/(?:US\\$|\\$)\\s*([\\d.,]+)/g)).map(m => parseFloat(m[1].replace(/,/g,'')));
                return arr.filter(x => !isNaN(x));
              };
              return nodes.map((el, idx) => {
                const name = (el.querySelector("input[name='prdtName']")?.value || '').trim();
                const brand = (el.querySelector("dl.brand-info dt, .brand, .brand_name, .brandName")?.textContent || '').replace(/\\s+/g,' ').trim();
                const a = el.querySelector("a");
                const link = a && a.href ? a.href : '';
                const rankTxt = (el.querySelector(".rank-badge span, .rank_num")?.textContent||'').trim();
                const rank = parseInt(rankTxt) || (idx+1);

                const pbox = el.querySelector(".price-info") || el;
                const ptxt = (pbox.textContent||'').replace(/\\s+/g,' ').trim();
                const amts = priceNums(ptxt);
                let sale=null, orig=null;
                if (amts.length==1){ sale=amts[0]; }
                if (amts.length>=2){ sale=Math.min(...amts); orig=Math.max(...amts); }

                const saleTxt = (el.querySelector(".price-info strong.point")?.textContent||'').trim();
                const origTxt = (el.querySelector(".price-info span")?.textContent||'').trim();
                if (sale==null && saleTxt){ const v=parseFloat(saleTxt.replace(/[^\\d.]/g,'')); if(!isNaN(v)) sale=v; }
                if (orig==null && origTxt){ const v=parseFloat(origTxt.replace(/[^\\d.]/g,'')); if(!isNaN(v)) orig=v; }

                const pctTxt = (el.querySelector(".price-info .rate, .discount-rate, .percent, .dc")?.textContent||'').trim();

                return {rank, brand, name, link, sale, orig, pctTxt};
              }).filter(x => x.name && x.link);
            }
            """,
            CARD_SEL
        )

        context.close(); browser.close()

    products: List[Product] = []
    for row in data:
        pct = discount_floor(row.get("orig"), row.get("sale"), row.get("pctTxt"))
        products.append(Product(
            rank=int(row.get("rank") or 0) or None,
            brand=clean_text(row.get("brand") or ""),
            title=clean_text(row.get("name") or ""),
            price=row.get("sale"),
            orig_price=row.get("orig"),
            discount_percent=pct,
            url=row.get("link") or "",
        ))
    return products

def fetch_products() -> List[Product]:
    # 1) HTTP 시도
    try:
        items = fetch_by_http()
        if len(items) >= 10:
            return items
    except Exception as e:
        print("[HTTP 오류] → Playwright 폴백:", e)

    # 2) Playwright 시도
    return fetch_by_playwright()

# ---------------- Google Drive 업/다운 ----------------
def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials

    sa_json = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    scopes = ["https://www.googleapis.com/auth/drive"]
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        cid  = os.getenv("GOOGLE_CLIENT_ID")
        csec = os.getenv("GOOGLE_CLIENT_SECRET")
        rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
        if not (cid and csec and rtk):
            raise RuntimeError("Google Drive 자격정보가 없습니다.")
        creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                            client_id=cid, client_secret=csec, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)").execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        return file_id
    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id").execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
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
    fh.seek(0)
    return pd.read_csv(fh)

# ---------------- Slack ----------------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력")
        print(text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ---------------- 비교/메시지 ----------------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    rows = []
    for p in products:
        rows.append({
            "date": date_str,
            "rank": p.rank,
            "brand": p.brand,
            "product_name": p.title,
            "price": p.price,
            "orig_price": p.orig_price,
            "discount_percent": p.discount_percent,
            "url": p.url,
            "otuk": False if p.rank is not None else True,  # 국내 버전과 동일 컬럼 유지
        })
    return pd.DataFrame(rows)

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

    df_t = df_today.copy(); df_t["key"] = df_t["url"]; df_t.set_index("key", inplace=True)
    if df_prev is not None and len(df_prev):
        df_p = df_prev.copy(); df_p["key"] = df_p["url"]; df_p.set_index("key", inplace=True)
    else:
        df_p = pd.DataFrame(columns=df_t.columns)

    # TOP10
    top10 = df_t.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        name_only = remove_brand_from_title(r["product_name"], r.get("brand", ""))
        name_link = f"<{r['url']}|{slack_escape(name_only)}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent")
        tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        S["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    # Top30 비교
    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()
    common = set(t30.index) & set(p30.index)
    new    = set(t30.index) - set(p30.index)
    out    = set(p30.index) - set(t30.index)

    # 🔥 급상승: 개선폭 desc, 상위 3 (동률: 오늘순위 asc → 전일순위 asc → 이름 가나다)
    rising = []
    for k in common:
        prev_rank = int(p30.loc[k, "rank"]); curr_rank = int(t30.loc[k, "rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            nm = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
            link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
            line, _ = line_move(link, prev_rank, curr_rank)
            rising.append((imp, curr_rank, prev_rank, slack_escape(nm), line))
    rising.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["rising"] = [e[-1] for e in rising[:3]]

    # 🆕 뉴랭커: 전일 Top30 밖 → 오늘 Top30 진입(≤30), 오늘순위 asc, 최대 3
    newcomers = []
    for k in new:
        curr_rank = int(t30.loc[k, "rank"])
        nm = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
        link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
        newcomers.append((curr_rank, f"- {link} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [line for _, line in newcomers[:3]]

    # 📉 급하락: 하락폭 desc, 상위 5
    falling = []
    for k in common:
        prev_rank = int(p30.loc[k, "rank"]); curr_rank = int(t30.loc[k, "rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            nm = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
            link = f"<{t30.loc[k,'url']}|{slack_escape(nm)}>"
            line, _ = line_move(link, prev_rank, curr_rank)
            falling.append((drop, curr_rank, prev_rank, slack_escape(nm), line))
    falling.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    S["falling"] = [e[-1] for e in falling[:5]]

    # OUT (전일 Top30 → 오늘 Top30 밖)
    for k in sorted(list(out)):
        prev_rank = int(p30.loc[k, "rank"])
        nm = remove_brand_from_title(p30.loc[k, "product_name"], p30.loc[k].get("brand", ""))
        link = f"<{p30.loc[k,'url']}|{slack_escape(nm)}>"
        line, _ = line_move(link, prev_rank, None)
        S["outs"].append(line)

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    parts = []
    parts.append(f"*올리브영 글로벌몰 랭킹 — {date_str}*")
    parts.append("")
    parts.append("*TOP 10*")
    parts += S["top10"] or ["- 데이터 없음"]
    parts.append("")
    parts.append("*🔥 급상승*")
    parts += S["rising"] or ["- 해당 없음"]
    parts.append("")
    parts.append("*🆕 뉴랭커*")
    parts += S["newcomers"] or ["- 해당 없음"]
    parts.append("")
    parts.append("*📉 급하락*")
    parts += S["falling"] or ["- 해당 없음"]
    for line in S.get("outs", []):
        parts.append(line)
    parts.append("")
    parts.append("*🔄 랭크 인&아웃*")
    parts.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(parts)

# ---------------- 메인 ----------------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)

    items: List[Product] = []
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

    # 로컬 저장(진행 확인용)
    os.makedirs("data", exist_ok=True)
    local_path = os.path.join("data", file_today)
    df_today.to_csv(local_path, index=False, encoding="utf-8-sig")
    print("로컬 저장:", local_path)

    # Google Drive 업로드 + 전일 파일 다운로드
    drive_folder = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    df_prev = None
    if drive_folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, drive_folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, drive_folder, file_yesterday)
            if df_prev is not None:
                print("전일 CSV 다운로드 성공:", file_yesterday)
            else:
                print("전일 CSV 미발견:", file_yesterday)
        except Exception as e:
            print("Google Drive 처리 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    sections = build_sections(df_today, df_prev)
    message = build_slack_message(date_str, sections)
    slack_post(message)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e)
        traceback.print_exc()
        try:
            slack_post(f"*올리브영 글로벌몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except:
            pass
        raise
