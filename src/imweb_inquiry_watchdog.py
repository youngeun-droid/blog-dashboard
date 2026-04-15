import os
from datetime import datetime
from zoneinfo import ZoneInfo

from slack import post_webhook


KST = ZoneInfo("Asia/Seoul")
DEFAULT_STATE_PATH = "data/imweb_inquiry_last_success.txt"
DEFAULT_ALERT_STATE_PATH = "data/imweb_inquiry_last_alert.txt"


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _read_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as fp:
        return fp.read().strip()


def _write_text(path: str, value: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(value)


def main() -> None:
    now = datetime.now(tz=KST)
    # Monday only
    if now.weekday() != 0:
        return

    # Check after 11:40 KST
    if (now.hour, now.minute) < (11, 40):
        return

    webhook_url = _env("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise SystemExit("SLACK_WEBHOOK_URL 환경변수가 필요합니다.")

    state_path = _env("IMWEB_REPORT_STATE_PATH", DEFAULT_STATE_PATH)
    alert_state_path = _env("IMWEB_ALERT_STATE_PATH", DEFAULT_ALERT_STATE_PATH)

    last_success_raw = _read_text(state_path)
    today = now.date().isoformat()
    already_alerted = _read_text(alert_state_path) == today

    if not last_success_raw:
        if not already_alerted:
            post_webhook(
                webhook_url,
                "*[알림] 닥터팔레트 도입문의 주간 리포트 미발송*\n"
                f"- 기준일: {today} (월요일)\n"
                "- 상태: 성공 이력이 없습니다.\n"
                "- 점검: `out/launchd_imweb_inquiry.err` 로그 확인 필요",
            )
            _write_text(alert_state_path, today)
        return

    try:
        last_success = datetime.fromisoformat(last_success_raw)
    except ValueError:
        if not already_alerted:
            post_webhook(
                webhook_url,
                "*[알림] 닥터팔레트 도입문의 주간 리포트 미발송*\n"
                f"- 기준일: {today} (월요일)\n"
                f"- 상태: 성공 상태 파일 파싱 실패 (`{last_success_raw}`)\n"
                "- 점검: `data/imweb_inquiry_last_success.txt` 형식 확인 필요",
            )
            _write_text(alert_state_path, today)
        return

    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=KST)
    else:
        last_success = last_success.astimezone(KST)

    if last_success.date().isoformat() == today:
        return

    if not already_alerted:
        post_webhook(
            webhook_url,
            "*[알림] 닥터팔레트 도입문의 주간 리포트 미발송*\n"
            f"- 기준일: {today} (월요일)\n"
            f"- 마지막 성공: {last_success.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            "- 점검: `out/launchd_imweb_inquiry.err` 로그 확인 필요",
        )
        _write_text(alert_state_path, today)


if __name__ == "__main__":
    main()
