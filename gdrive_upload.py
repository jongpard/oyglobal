# gdrive_upload.py
import os, sys, json, base64, mimetypes

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCreds
from google.oauth2.service_account import Credentials as SACreds

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

def _user_creds_from_env():
    cid = (os.getenv("GOOGLE_CLIENT_ID") or "").strip()
    csec = (os.getenv("GOOGLE_CLIENT_SECRET") or "").strip()
    rtok = (os.getenv("GOOGLE_REFRESH_TOKEN") or "").strip()
    if cid and csec and rtok:
        creds = UserCreds(
            token=None,
            refresh_token=rtok,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cid,
            client_secret=csec,
            scopes=SCOPES,
        )
        creds.refresh(Request())
        return creds
    return None

def _sa_creds_from_env():
    b64 = (os.getenv("GDRIVE_SA_JSON_B64") or "").strip()
    js = (os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    if b64:
        js = base64.b64decode(b64).decode("utf-8")
    if js:
        info = json.loads(js)
        return SACreds.from_service_account_info(info, scopes=SCOPES)
    return None

def _get_creds():
    # 1) OAuth 사용자 자격증명(개인 My Drive 업로드용) — 국내몰에서 쓰던 방식
    creds = _user_creds_from_env()
    if creds:
        print("🔑 Using OAuth user credentials.")
        return creds
    # 2) 서비스계정(Shared Drive 업로드 전용)
    creds = _sa_creds_from_env()
    if creds:
        print("🔑 Using Service Account credentials.")
        return creds
    raise SystemExit(
        "No Google Drive credentials. "
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
        # Shared Drive/개인 드라이브 모두 안전하게 처리
        supportsAllDrives=True,
    ).execute()

    print(f"✅ Uploaded to Google Drive: {created.get('name')} (id={created.get('id')})")
    if created.get("webViewLink"):
        print(f"🔗 {created['webViewLink']}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python gdrive_upload.py <csv_path>")
        sys.exit(1)
    folder_id = (os.getenv("GDRIVE_FOLDER_ID") or "").strip()
    upload_to_drive(sys.argv[1], folder_id)
