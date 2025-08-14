from datetime import datetime
import os
import pytz

KST = pytz.timezone("Asia/Seoul")

def kst_today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def ensure_data_dir():
    os.makedirs("data", exist_ok=True)
