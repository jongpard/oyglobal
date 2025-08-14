# slack_notify.py
import json
import requests

def post_slack_message(webhook_url: str, blocks, fallback_text: str = "") -> None:
    """
    기존 main.py가 사용하는 함수 시그니처를 복원.
    - webhook_url: Slack Incoming Webhook URL
    - blocks: Block Kit 배열 (list[dict])
    - fallback_text: 앱이 블록을 지원하지 않을 때 표시할 텍스트
    """
    payload = {
        "text": fallback_text or " ",
        "blocks": blocks if isinstance(blocks, list) else [],
    }
    try:
        r = requests.post(webhook_url, data=json.dumps(payload),
                          headers={"Content-Type": "application/json"}, timeout=20)
        r.raise_for_status()
        print("[INFO] Slack 전송 성공")
    except Exception as e:
        print(f"[WARN] Slack 전송 실패: {e}")
