# gdrive_upload.py
import os, sys, json, base64, mimetypes
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCreds
from google.oauth2.service_account import Credentials as SACreds
from google.auth.exceptions import RefreshError

SCOPES_FULL = ["https://www.googleapis.com/auth/drive"]
SCOPES_FILE = ["https://www.googleapis.com/auth/drive.file"]

def _user_creds_from_env():
    cid  = (os.getenv("GOOGLE_CLIENT_ID") or "").strip()
    csec = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    rtok = (os.getenv("GOOGLE_REFRESH_TOKEN") or "").strip()
    if not (cid and csec and rtok):
        return None

    # 1) 스코프 미지정(토큰에 내장된 스코프 사용)으로 먼저 갱신
    try:
        creds = UserCreds(
            token=None,
            refresh_token=rtok,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cid,
            client_secret=csec,
            scopes=None,   # ⬅️ 중요: 기존 리프레시 토큰의 스코프를 그대로 사용
        )
        creds.refresh(Request())
        print("🔑 Using OAuth user credentials (original scopes).")
        return creds
    except RefreshError as e1:
        # 2) drive.file 단일 스코프로 재시도 (국내몰에서 이 조합을 많이 사용)
        try:
            creds = UserCreds(
                token=None,
                refresh_token=rtok,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=cid,
                client_secret=csec,
                scopes=SCOPES_FILE,
            )
            creds.refresh(Request())
            print("🔑 Using OAuth user credentials (drive.file).")
            return creds
        except RefreshError as e2:
            print(f"⚠️ OAuth refresh failed: {e1}. retry:{e2}")
            return None

def _sa_creds_from_env():
    b64 = (os.getenv("GDRIVE_SA_JSON_B64") or "").strip()
    js  = (os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    if b64:
        js = base64.b64decode(b64).decode("utf-8")
    if not js:
        return None
    info = json.loads(js)
    print("🔑 Using Service Account credentials.")
    return SACreds.from_service_account_info(info, scopes=SCOPES_FILE + SCOPES_FULL)

def _get_creds():
    # 1) OAuth 우선 (My Drive 업로드용)
    user = _user_creds_from_env()
    if user:
        return user
    # 2) 서비스계정(Shared Drive 업로드용)
    sa = _sa_creds_from_env()
    if sa:
        return sa
    raise SystemExit(
        "No Google Drive credentials found. "
        "Set GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN (recommended), "
        "or GDRIVE_SA_JSON_B64 / GDRIVE_SERVICE_ACCOUNT_JSON."
    )

def upload_to_drive(file_path: str, folder_id: str):
    if not os.path.exists(file_path):
        raise SystemExit(f"CSV not found: {file_path}")
    if not folder_id:
        raise SystemExit("Missing env: GDRIVE_FOLDER_ID")

    creds = _get_creds()
    service = build("drive", "v3", credentials=creds)

    file_name = os.path.basename(file_path)
    mime_type = mimetypes.guess_type(file_path)[0] or "text/csv"

    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink",
        supportsAllDrives=True,  # 개인/공유 드라이브 모두 대응
    ).execute()

    print(f"✅ Uploaded to Google Drive: {created.get('name')} (id={created.get('id')})")
    if created.get("webViewLink"):
        print(f"🔗 {created['webViewLink']}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python gdrive_upload.py <csv_path>")
        sys.exit(1)
    upload_to_drive(sys.argv[1], (os.getenv("GDRIVE_FOLDER_ID") or "").strip())
