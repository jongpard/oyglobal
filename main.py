import os
import sys
import traceback
from utils import (
    get_kst_today_str, ensure_dirs, load_previous_csv, save_today_csv,
    compute_diffs_and_blocks, gdrive_upload_oauth
)
from oy_global import scrape_oy_global_us
from slack_notify import post_slack_message

def main():
    ensure_dirs()
    today_str = get_kst_today_str()
    today_csv = f"data/{today_str}_global.csv"

    try:
        df_today = scrape_oy_global_us(debug=os.getenv("OY_DEBUG") == "1")
        if df_today is None or df_today.empty:
            raise RuntimeError("크롤링 결과가 비어 있습니다. 셀렉터/구조 변경 가능성 확인 필요")

        save_today_csv(df_today, today_csv)

        df_prev, prev_path = load_previous_csv(today_csv)
        blocks, text = compute_diffs_and_blocks(df_today, df_prev, prev_path)

        webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
        if webhook:
            post_slack_message(webhook, blocks, fallback_text=text)

        # === Google Drive 업로드 (OAuth) ===
        folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
        cid = os.getenv("GOOGLE_CLIENT_ID", "").strip()
        csecret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
        refresh = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
        if folder_id and cid and csecret and refresh:
            gdrive_upload_oauth(today_csv, folder_id, cid, csecret, refresh)

        print("✅ 완료")

    except Exception as e:
        print("❌ 실패:", e)
        traceback.print_exc()
        webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
        if webhook:
            post_slack_message(
                webhook,
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*OY Global 크롤링 실패*\n```{str(e)}```"}
                }],
                fallback_text=f"OY Global 크롤링 실패: {str(e)}"
            )
        sys.exit(1)

if __name__ == "__main__":
    main()
