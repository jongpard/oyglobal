# gdrive_upload.py
import os, sys, json, mimetypes
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_to_drive(file_path: str, folder_id: str, creds_json: str):
    creds_info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=creds)

    file_name = os.path.basename(file_path)
    mime_type = mimetypes.guess_type(file_path)[0] or "text/csv"

    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
    created = service.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
    print(f"✅ Uploaded to Google Drive: {created.get('name')} (id={created.get('id')})")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python gdrive_upload.py <csv_path>")
        sys.exit(1)
    csv_path = sys.argv[1]
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    creds_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON")
    if not folder_id or not creds_json:
        raise SystemExit("Missing env: GDRIVE_FOLDER_ID or GDRIVE_SERVICE_ACCOUNT_JSON")
    upload_to_drive(csv_path, folder_id, creds_json)
