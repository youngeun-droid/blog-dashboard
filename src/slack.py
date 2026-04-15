from typing import Dict

import requests


def post_webhook(webhook_url: str, text: str) -> Dict:
    payload = {"text": text}
    response = requests.post(webhook_url, json=payload, timeout=30)
    response.raise_for_status()
    if not response.content:
        return {"ok": True}
    # Slack incoming webhook usually returns plain-text "ok", not JSON.
    try:
        return response.json()
    except ValueError:
        return {"ok": response.text.strip().lower() == "ok", "raw": response.text}
