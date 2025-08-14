import json, requests
def post_slack_message(webhook_url: str, blocks, fallback_text: str = ""):
    payload = {"text": fallback_text or "OY Global Ranking", "blocks": blocks}
    r = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type":"application/json"}, timeout=30)
    if r.status_code >= 300:
        print("[WARN] Slack 전송 실패:", r.status_code, r.text[:200])
    else:
        print("[INFO] Slack 전송 성공")
