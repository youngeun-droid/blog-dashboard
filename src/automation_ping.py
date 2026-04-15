import datetime as dt
from pathlib import Path

from dotenv import load_dotenv

from naver_place_review_alert import post_webhook


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"[자동화 핑] 실행 확인: {ts}"
    # post_webhook raises on failure, which is fine for visibility.
    from os import getenv

    url = getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        raise RuntimeError("SLACK_WEBHOOK_URL missing")
    post_webhook(url, message)


if __name__ == "__main__":
    main()
