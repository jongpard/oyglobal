# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 수집/비교/알림 (USD)
- 데이터 소스: https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1
- HTTP 우선 → 결과 부족 시 Playwright 폴백
- 저장 파일명: 올리브영글로벌_랭킹_YYYY-MM-DD.csv (KST 기준)
- 전일 CSV와 비교하여 TOP10/급상승/뉴랭커/급하락(OUT 포함)/랭크 인&아웃 계산
- Slack 메시지: 국내 버전과 동일한 구조/포맷(모든 제목/소제목 굵게)
- Google Drive 업로드/전일 파일 조회 (서비스계정 or OAuth RefreshToken 모두 지원)
- 환경변수:
  SLACK_WEBHOOK_URL
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN (OAuth 사용 시)
  GDRIVE_FOLDER_ID
  GDRIVE_SERVICE_ACCOUNT_JSON (서비스계정 JSON 문자열; 있으면 이걸 우선 사용)
"""
import os
import re
import io
import math
import json
import time
import pytz
import uuid
import traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ==== 환경 상수 ====
BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

# ==== 유틸 ====
def now_kst() -> dt.datetime:
    return dt.datetime.now(KST)

def today_kst_str() -> str:
    return now_kst().strftime("%Y-%m-%d")

def yesterday_kst_str() -> str:
    y = now_kst() - dt.timedelta(days=1)
    return y.strftime("%Y-%m-%d")

def build_filename(date_str: str) -> str:
    return f"올리브영글로벌_랭킹_{date_str}.csv"

def to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.findall(r"[\d]+(?:\.[\d]+)?", str(s))
    if not m:
        return None
    try:
        return float(m[0])
    except:
        return None

def extract_percent_floor(orig_price: Optional[float], sale_price: Optional[float], percent_text: Optional[str]) -> Optional[int]:
    """
    우선순위:
      1) percent_text에서 숫자 추출해 내림
      2) 원가/판매가가 있으면 계산해 내림
    """
    # 1) 직접 표기된 퍼센트
    if percent_text:
        n = to_float(percent_text)
        if n is not None:
            return int(math.floor(n))
    # 2) 가격으로 계산
    if orig_price and sale_price and orig_price > 0:
        pct = (1 - (sale_price / orig_price)) * 100.0
        return max(0, int(math.floor(pct)))
    return None

def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def remove_brand_from_title(title: str, brand: str) -> str:
    """TOP10 표시에 브랜드 제거 (국내 버전 규칙 동일)"""
    t = clean_text(title)
    b = clean_text(brand)
    if not b:
        return t
    # [브랜드] 제품명 / 브랜드 제품명 / (브랜드) 제품명 등 선두부 제거
    patterns = [
        rf"^\[?\s*{re.escape(b)}\s*\]?\s*[-–—:|]*\s*",
        rf"^\(?\s*{re.escape(b)}\s*\)?\s*[-–—:|]*\s*",
    ]
    for pat in patterns:
        t2 = re.sub(pat, "", t, flags=re.I)
        if t2 != t:
            t = t2.strip()
            break
    return t

def slack_escape(s: str) -> str:
    # Slack의 링크 텍스트에는 &, <, > 이슈만 간단히 처리
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_currency_usd(v: Optional[float]) -> str:
    if v is None:
        return "$0.00"
    return f"${v:,.2f}"

@dataclass
class Product:
    rank: Optional[int]             # 1-based
    brand: str
    title: str
    price: Optional[float]          # 판매가(할인가)
    orig_price: Optional[float]     # 정상가(있으면)
    discount_percent: Optional[int] # 소수점 없이 버림
    url: str

def parse_cards_from_html(html: str) -> List[Product]:
    """
    HTTP 응답의 HTML에서 최대한 파싱 (전면 JS일 수도 있으므로 실패 가능)
    다양한 클래스/구조를 포괄적으로 시도
    """
    soup = BeautifulSoup(html, "lxml")

    # 후보 셀렉터들 (상황 변화 대응)
    item_selectors = [
        "ul.tab_cont_list li",
        "ul.best_list li",
        "ul#bestSellerContent li",
        "ul li.prod_item",
        "ul li",
        "div.prod_area",
    ]

    name_selectors = [".product_name", ".prod_name", ".name", ".tit", ".tx_name", ".item_name", "a[title]"]
    brand_selectors = [".brand", ".brand_name", ".tx_brand", ".brandName"]
    link_selectors = ["a", "a.prod_link", "a.link", "a.detail_link"]
    price_selectors = [".price .num", ".sale_price", ".discount_price", ".final_price", ".price", ".value"]
    orig_price_selectors = [".orig_price", ".normal_price", ".consumer", ".strike", ".was"]
    percent_selectors = [".percent", ".dc", ".discount_rate", ".rate"]

    def pick_text(el, selectors):
        for sel in selectors:
            node = el.select_one(sel)
            if node:
                t = clean_text(node.get_text(" ", strip=True))
                if t:
                    return t
        return ""

    def pick_link(el, selectors):
        for sel in selectors:
            a = el.select_one(sel)
            if a and a.has_attr("href"):
                href = a["href"].strip()
                if href and not href.startswith("javascript"):
                    return href
        return ""

    items: List[Product] = []
    found = []
    for sel in item_selectors:
        found = soup.select(sel)
        if found and len(found) >= 10:
            break

    # 순위는 DOM 순서로 매기되, 카드가 부족하면 빈 리스트 유지 (폴백 유도)
    for idx, li in enumerate(found, start=1):
        title = pick_text(li, name_selectors)
        brand = pick_text(li, brand_selectors)
        link = pick_link(li, link_selectors)
        price_txt = pick_text(li, price_selectors)
        orig_txt = pick_text(li, orig_price_selectors)
        pct_txt = pick_text(li, percent_selectors)

        sale = to_float(price_txt)
        orig = to_float(orig_txt)
        pct = extract_percent_floor(orig, sale, pct_txt)

        # 상대경로 보정
        if link and link.startswith("/"):
            link = "https://global.oliveyoung.com" + link

        if title and link:
            items.append(Product(
                rank=idx,
                brand=brand,
                title=title,
                price=sale,
                orig_price=orig,
                discount_percent=pct,
                url=link
            ))

    return items

def fetch_by_http() -> List[Product]:
    hdrs = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        # 미국 사용자로 위장
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=hdrs, timeout=20)
    r.raise_for_status()
    return parse_cards_from_html(r.text)

def fetch_by_playwright() -> List[Product]:
    """
    Playwright 폴백. 다양한 셀렉터 조합을 사용하여 카드 요소를 수집.
    - locale='en-US', timezone='America/Los_Angeles' 로 글로벌몰/달러 유도
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            timezone_id="America/Los_Angeles",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.goto(BEST_URL, wait_until="networkidle", timeout=60_000)

        # 동적 로드 대기: 여러 후보 셀렉터 중 하나가 나타나면 OK
        candidates = [
            "ul.tab_cont_list li",
            "ul#bestSellerContent li",
            "ul.best_list li",
            "div.best_seller_wrap li",
            "li .product_name",
        ]

        success = False
        for sel in candidates:
            try:
                page.wait_for_selector(sel, timeout=30_000)
                success = True
                break
            except:
                pass
        if not success:
            # 한 번 더 스크롤 유도
            page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
            page.wait_for_selector(candidates[-1], timeout=30_000)

        # 브라우저 내에서 넓게 긁어오기 (다양한 클래스 대응)
        data = page.evaluate(
            """
            () => {
              const pick = (el, sels) => {
                for (const s of sels) {
                  const x = el.querySelector(s);
                  if (x) {
                    const t = (x.textContent || '').trim();
                    if (t) return t;
                  }
                }
                return '';
              };
              const pickLink = (el, sels) => {
                for (const s of sels) {
                  const a = el.querySelector(s);
                  if (a && a.href) return a.href;
                }
                return '';
              };
              const itemSelectors = [
                'ul.tab_cont_list li',
                'ul#bestSellerContent li',
                'ul.best_list li',
                'ul li.prod_item',
                'ul li',
                'div.prod_area',
              ];
              const nameSelectors = ['.product_name', '.prod_name', '.name', '.tit', '.tx_name', '.item_name', 'a[title]'];
              const brandSelectors = ['.brand', '.brand_name', '.tx_brand', '.brandName'];
              const linkSelectors = ['a', 'a.prod_link', 'a.link', 'a.detail_link'];
              const priceSelectors = ['.price .num', '.sale_price', '.discount_price', '.final_price', '.price', '.value'];
              const origSelectors  = ['.orig_price', '.normal_price', '.consumer', '.strike', '.was'];
              const percentSelectors = ['.percent', '.dc', '.discount_rate', '.rate'];

              let nodes = [];
              for (const s of itemSelectors) {
                const found = Array.from(document.querySelectorAll(s));
                if (found.length >= 10) { nodes = found; break; }
                if (!nodes.length) nodes = found;
              }
              return nodes.map((el, idx) => {
                const title = pick(el, nameSelectors);
                const brand = pick(el, brandSelectors);
                const link = pickLink(el, linkSelectors);
                const price = pick(el, priceSelectors);
                const orig  = pick(el, origSelectors);
                const pct   = pick(el, percentSelectors);
                return {rank: idx + 1, title, brand, link, price, orig, pct};
              }).filter(x => x.title && x.link);
            }
            """
        )
        context.close()
        browser.close()

    products: List[Product] = []
    for row in data:
        sale = to_float(row.get("price"))
        orig = to_float(row.get("orig"))
        pct = extract_percent_floor(orig, sale, row.get("pct"))

        products.append(Product(
            rank=row.get("rank"),
            brand=clean_text(row.get("brand")),
            title=clean_text(row.get("title")),
            price=sale,
            orig_price=orig,
            discount_percent=pct,
            url=row.get("link"),
        ))
    return products

def fetch_products() -> List[Product]:
    # 1) HTTP 시도
    try:
        items = fetch_by_http()
        if len(items) >= 20:
            return items
    except Exception as e:
        print("[HTTP] 실패/부족 → Playwright 폴백:", e)

    # 2) Playwright 폴백
    items = fetch_by_playwright()
    return items

# ==== Google Drive ====
def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials

    sa_json = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = None

    if sa_json:
        # 서비스계정 우선
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        # OAuth Refresh Token
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        refresh_token = os.getenv("GOOGLE_REFRESH_TOKEN")
        if not (client_id and client_secret and refresh_token):
            raise RuntimeError("Google Drive 자격정보가 없습니다.")
        creds = Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
        )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload

    # 같은 이름이 있으면 업데이트, 없으면 생성
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id, name)").execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)

    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        return file_id
    else:
        meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
        created = service.files().create(body=meta, media_body=media, fields="id").execute()
        return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id, name)").execute()
    files = res.get("files", [])
    if not files:
        return None
    file_id = files[0]["id"]

    req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    return pd.read_csv(fh)

# ==== Slack ====
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL이 설정되지 않았습니다. 메시지를 출력으로 대체합니다.")
        print(text)
        return
    r = requests.post(url, json={"text": text}, timeout=15)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

# ==== 비교/섹션 계산 ====
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    rows = []
    for p in products:
        rows.append({
            "date": date_str,
            "rank": p.rank,
            "brand": p.brand,
            "product_name": p.title,
            "price": p.price,                 # 숫자 (달러)
            "orig_price": p.orig_price,       # 숫자
            "discount_percent": p.discount_percent,  # 정수(버림)
            "url": p.url,
            "otuk": False if p.rank is not None else True,  # 국내 버전과 동일한 컬럼 유지
        })
    return pd.DataFrame(rows)

def line_move(name_link: str, prev_rank: Optional[int], curr_rank: Optional[int]) -> Tuple[str, int]:
    """
    포맷 라인과 이동폭(절대값) 반환
    """
    if prev_rank is None and curr_rank is not None:
        return f"- {name_link} NEW → {curr_rank}위", 99999
    if curr_rank is None and prev_rank is not None:
        return f"- {name_link} {prev_rank}위 → OUT", 99999
    if prev_rank is None or curr_rank is None:
        return f"- {name_link}", 0

    delta = prev_rank - curr_rank
    if delta > 0:
        return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↑{delta})", delta
    elif delta < 0:
        return f"- {name_link} {prev_rank}위 → {curr_rank}위 (↓{abs(delta)})", abs(delta)
    else:
        return f"- {name_link} {prev_rank}위 → {curr_rank}위 (변동없음)", 0

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    """
    섹션별 라인 문자열 리스트 생성
    """
    sections = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}

    # 정렬/키
    df_t = df_today.copy()
    df_t["key"] = df_t["url"]  # 고유키로 링크 사용 (국내 버전과 동일 전략)
    df_t.set_index("key", inplace=True)

    if df_prev is not None and len(df_prev):
        df_p = df_prev.copy()
        df_p["key"] = df_p["url"]
        df_p.set_index("key", inplace=True)
    else:
        df_p = pd.DataFrame(columns=df_t.columns)

    # ---- TOP 10 ----
    top10 = df_t.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        name_only = remove_brand_from_title(r["product_name"], r.get("brand", ""))
        name_link = f"<{r['url']}|{slack_escape(name_only)}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent")
        tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        sections["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    # ---- 집합/랭크 정보 준비 ----
    # Top30만 비교 대상
    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()

    # 공통 / 신규 / 아웃 판별
    common_keys = set(t30.index).intersection(set(p30.index))
    new_keys = set(t30.index) - set(p30.index)  # 전일 Top30 밖 → 오늘 Top30
    out_keys = set(p30.index) - set(t30.index)  # 전일 Top30 → 오늘 Top30 밖(or 미등장)

    # ---- 급상승 (공통 중 개선폭 > 0, 상위 3) ----
    rising_candidates = []
    for k in common_keys:
        prev_rank = int(p30.loc[k, "rank"])
        curr_rank = int(t30.loc[k, "rank"])
        imp = prev_rank - curr_rank
        if imp > 0:
            name_only = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
            name_link = f"<{t30.loc[k, 'url']}|{slack_escape(name_only)}>"
            line, _ = line_move(name_link, prev_rank, curr_rank)
            rising_candidates.append((imp, curr_rank, prev_rank, slack_escape(name_only), line))
    # 정렬: 개선폭 desc → 오늘순위 asc → 전일순위 asc → 제품명 가나다
    rising_candidates.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    for entry in rising_candidates[:3]:
        sections["rising"].append(entry[-1])

    # ---- 뉴랭커 (전일 Top30 밖 → 오늘 Top30 진입), 오늘순위 asc, 최대 3 ----
    newcomers = []
    for k in new_keys:
        curr_rank = int(t30.loc[k, "rank"])
        name_only = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
        name_link = f"<{t30.loc[k, 'url']}|{slack_escape(name_only)}>"
        newcomers.append((curr_rank, f"- {name_link} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    for _, line in newcomers[:3]:
        sections["newcomers"].append(line)

    # ---- 급하락 (공통 중 하락폭 > 0, 내림차순 상위 5) + OUT 같이 표기 ----
    falling_candidates = []
    for k in common_keys:
        prev_rank = int(p30.loc[k, "rank"])
        curr_rank = int(t30.loc[k, "rank"])
        drop = curr_rank - prev_rank
        if drop > 0:
            name_only = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
            name_link = f"<{t30.loc[k, 'url']}|{slack_escape(name_only)}>"
            line, _ = line_move(name_link, prev_rank, curr_rank)
            falling_candidates.append((drop, curr_rank, prev_rank, slack_escape(name_only), line))
    falling_candidates.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    for entry in falling_candidates[:5]:
        sections["falling"].append(entry[-1])

    # OUT (전일 Top30 → 오늘 Top30 밖)
    for k in sorted(list(out_keys)):
        prev_rank = int(p30.loc[k, "rank"])
        name_only = remove_brand_from_title(p30.loc[k, "product_name"], p30.loc[k].get("brand", ""))
        name_link = f"<{p30.loc[k, 'url']}|{slack_escape(name_only)}>"
        line, _ = line_move(name_link, prev_rank, None)
        sections["outs"].append(line)

    # ---- 랭크 인&아웃 개수 ----
    sections["inout_count"] = len(new_keys) + len(out_keys)
    return sections

def build_slack_message(date_str: str, sections: Dict[str, List[str]]) -> str:
    parts = []
    parts.append(f"*올리브영 글로벌몰 랭킹 — {date_str}*")
    parts.append("")
    # TOP 10
    parts.append("*TOP 10*")
    if sections["top10"]:
        parts += [f"{line}" for line in sections["top10"]]
    else:
        parts.append("- 데이터 없음")

    # 급상승
    parts.append("")
    parts.append("*🔥 급상승*")
    if sections["rising"]:
        parts += sections["rising"]
    else:
        parts.append("- 해당 없음")

    # 뉴랭커
    parts.append("")
    parts.append("*🆕 뉴랭커*")
    if sections["newcomers"]:
        parts += sections["newcomers"]
    else:
        parts.append("- 해당 없음")

    # 급하락 (5개) + OUT
    parts.append("")
    parts.append("*📉 급하락*")
    if sections["falling"]:
        parts += sections["falling"]
    else:
        parts.append("- 해당 없음")
    # OUT 함께 표기
    for line in sections.get("outs", []):
        parts.append(line)

    # 랭크 인&아웃
    parts.append("")
    parts.append("*🔄 랭크 인&아웃*")
    parts.append(f"{sections.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")

    return "\n".join(parts)

def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    products = fetch_products()
    print(f"수집 완료: {len(products)}개")

    if len(products) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터 점검 필요")

    df_today = to_dataframe(products, date_str)

    # CSV 로컬 저장 (워크플로 로그 확인용)
    os.makedirs("data", exist_ok=True)
    local_path = os.path.join("data", file_today)
    df_today.to_csv(local_path, index=False, encoding="utf-8-sig")
    print("로컬 저장:", local_path)

    # Google Drive 업로드 및 전일 파일 다운로드
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
            print("Google Drive 처리 중 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 건너뜀")

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
        # 실패 시에도 Slack에 간단히 알림 (선택)
        try:
            slack_post(f"*올리브영 글로벌몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except:
            pass
        raise
