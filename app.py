# -*- coding: utf-8 -*-
"""
올리브영 글로벌몰 베스트셀러 랭킹 수집/비교/알림 (USD)
"""
import os
import re
import io
import math
import json
import pytz
import traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

BEST_URL = "https://global.oliveyoung.com/display/page/best-seller?target=pillsTab1Nav1"
KST = pytz.timezone("Asia/Seoul")

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
    if percent_text:
        n = to_float(percent_text)
        if n is not None:
            return int(n // 1)
    if orig_price and sale_price and orig_price > 0:
        pct = (1 - (sale_price / orig_price)) * 100.0
        return max(0, int(pct // 1))
    return None

def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def remove_brand_from_title(title: str, brand: str) -> str:
    t = clean_text(title)
    b = clean_text(brand)
    if not b:
        return t
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
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_currency_usd(v: Optional[float]) -> str:
    if v is None:
        return "$0.00"
    return f"${v:,.2f}"

@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[float]
    orig_price: Optional[float]
    discount_percent: Optional[int]
    url: str

def parse_cards_from_html(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
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
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BEST_URL, headers=hdrs, timeout=25)
    r.raise_for_status()
    return parse_cards_from_html(r.text)

def fetch_by_playwright() -> List[Product]:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            timezone_id="America/Los_Angeles",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        )
        page = context.new_page()
        page.goto(BEST_URL, wait_until="domcontentloaded", timeout=60_000)
        # 네트워크 안정화
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except:
            pass

        # 쿠키/동의/배너 닫기 (여러 후보)
        for sel in [
            "button#onetrust-accept-btn-handler",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            "button:has-text('동의')",
            "button:has-text('확인')",
            "button[aria-label='Close']",
        ]:
            try:
                page.locator(sel).first.click(timeout=1500)
            except:
                pass

        # 자동 스크롤로 지연로딩 유도
        try:
            for _ in range(8):
                page.mouse.wheel(0, 2200)
                page.wait_for_timeout(900)
        except:
            pass

        # 넓은 셀렉터로 수집 (보임 여부 무시)
        data = page.evaluate(
            """
            () => {
              const pick = (el, sels) => {
                for (const s of sels) {
                  const x = el.querySelector(s);
                  if (x) {
                    const t = (x.textContent || '').replace(/\\s+/g,' ').trim();
                    if (t) return t;
                  }
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
                if (!nodes.length && found.length) nodes = found;
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

        # 폴백: JS 평가로도 10개 미만이면 HTML을 통째로 파싱
        products: List[Product] = []
        if not data or len(data) < 10:
            html = page.content()
            context.close()
            browser.close()
            return parse_cards_from_html(html)

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

        context.close()
        browser.close()
        return products

def fetch_products() -> List[Product]:
    try:
        items = fetch_by_http()
        if len(items) >= 10:   # 완화 (기존 20 → 10)
            return items
    except Exception as e:
        print("[HTTP] 실패/부족 → Playwright 폴백:", e)
    return fetch_by_playwright()

# ---------------- Google Drive / Slack / 계산 로직 (변경 없음) ----------------
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
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력 대체")
        print(text)
        return
    r = requests.post(url, json={"text": text}, timeout=15)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

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
            "otuk": False if p.rank is not None else True,
        })
    return pd.DataFrame(rows)

def line_move(name_link: str, prev_rank: Optional[int], curr_rank: Optional[int]) -> Tuple[str, int]:
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
    sections = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}
    df_t = df_today.copy()
    df_t["key"] = df_t["url"]
    df_t.set_index("key", inplace=True)
    if df_prev is not None and len(df_prev):
        df_p = df_prev.copy()
        df_p["key"] = df_p["url"]
        df_p.set_index("key", inplace=True)
    else:
        df_p = pd.DataFrame(columns=df_t.columns)

    top10 = df_t.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        name_only = remove_brand_from_title(r["product_name"], r.get("brand", ""))
        name_link = f"<{r['url']}|{slack_escape(name_only)}>"
        price_txt = fmt_currency_usd(r["price"])
        dc = r.get("discount_percent")
        tail = f" (↓{int(dc)}%)" if pd.notnull(dc) else ""
        sections["top10"].append(f"{int(r['rank'])}. {name_link} — {price_txt}{tail}")

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()
    common_keys = set(t30.index).intersection(set(p30.index))
    new_keys = set(t30.index) - set(p30.index)
    out_keys = set(p30.index) - set(t30.index)

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
    rising_candidates.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
    for entry in rising_candidates[:3]:
        sections["rising"].append(entry[-1])

    newcomers = []
    for k in new_keys:
        curr_rank = int(t30.loc[k, "rank"])
        name_only = remove_brand_from_title(t30.loc[k, "product_name"], t30.loc[k].get("brand", ""))
        name_link = f"<{t30.loc[k, 'url']}|{slack_escape(name_only)}>"
        newcomers.append((curr_rank, f"- {name_link} NEW → {curr_rank}위"))
    newcomers.sort(key=lambda x: x[0])
    for _, line in newcomers[:3]:
        sections["newcomers"].append(line)

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

    for k in sorted(list(out_keys)):
        prev_rank = int(p30.loc[k, "rank"])
        name_only = remove_brand_from_title(p30.loc[k, "product_name"], p30.loc[k].get("brand", ""))
        name_link = f"<{p30.loc[k, 'url']}|{slack_escape(name_only)}>"
        line, _ = line_move(name_link, prev_rank, None)
        sections["outs"].append(line)

    sections["inout_count"] = len(new_keys) + len(out_keys)
    return sections

def build_slack_message(date_str: str, sections: Dict[str, List[str]]) -> str:
    parts = []
    parts.append(f"*올리브영 글로벌몰 랭킹 — {date_str}*")
    parts.append("")
    parts.append("*TOP 10*")
    parts += sections["top10"] or ["- 데이터 없음"]
    parts.append("")
    parts.append("*🔥 급상승*")
    parts += sections["rising"] or ["- 해당 없음"]
    parts.append("")
    parts.append("*🆕 뉴랭커*")
    parts += sections["newcomers"] or ["- 해당 없음"]
    parts.append("")
    parts.append("*📉 급하락*")
    parts += sections["falling"] or ["- 해당 없음"]
    for line in sections.get("outs", []):
        parts.append(line)
    parts.append("")
    parts.append("*🔄 랭크 인&아웃*")
    parts.append(f"{sections.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(parts)

def slack_post_or_print(msg: str):
    try:
        slack_post(msg)
    except Exception as e:
        print("[Slack 오류]", e)
        print(msg)

def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("수집 시작:", BEST_URL)
    products = fetch_products()
    print(f"수집 완료: {len(products)}개")
    if len(products) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(products, date_str)

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
            print("전일 CSV", "다운로드 성공" if df_prev is not None else "미발견:", file_yesterday)
        except Exception as e:
            print("Google Drive 처리 중 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 건너뜀")

    sections = build_sections(df_today, df_prev)
    message = build_slack_message(date_str, sections)
    slack_post_or_print(message)
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
