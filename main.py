import os
import sys
import traceback
from utils import (
    get_kst_today_str, ensure_dirs, load_previous_csv, save_today_csv,
    compute_diffs_and_blocks, dropbox_upload_optional
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

        # 저장
        save_today_csv(df_today, today_csv)

        # 전일 로드
        df_prev, prev_path = load_previous_csv(today_csv)

        # 비교 & 슬랙 블록 생성
        blocks, text = compute_diffs_and_blocks(df_today, df_prev, prev_path)

        # 슬랙 전송
        webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
        if webhook:
            post_slack_message(webhook, blocks, fallback_text=text)
        else:
            print("[WARN] SLACK_WEBHOOK_URL이 없어 슬랙 전송을 건너뜁니다.")

        # (선택) Dropbox 업로드
        dropbox_token = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if dropbox_token:
            dropbox_upload_optional(dropbox_token, today_csv, f"/oyglobal/{os.path.basename(today_csv)}")

        print("✅ 완료")

    except Exception as e:
        print("❌ 실패:", e)
        traceback.print_exc()
        # 실패 상황에서도 (가능하면) 슬랙 알림
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
