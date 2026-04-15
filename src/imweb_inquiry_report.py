import os
import re
import imaplib
import email
import html as html_lib
import socket
from datetime import datetime, timedelta
from email.message import Message
from email.header import decode_header
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from slack import post_webhook


KST = ZoneInfo("Asia/Seoul")
DEFAULT_STATE_PATH = "data/imweb_inquiry_last_success.txt"


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _write_success_state(path: str, now: datetime) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(now.isoformat())


def _get_html_part(msg: Message) -> Optional[str]:
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    payload = msg.get_payload(decode=True)
    if payload:
        return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return None


def _html_to_lines(html: str) -> List[str]:
    # Convert common block tags to newlines, remove the rest.
    html = re.sub(r"<\s*br\s*/?\s*>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</\s*p\s*>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</\s*div\s*>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</\s*li\s*>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", html)
    text = html_lib.unescape(text)
    text = re.sub(r"\r", "", text)
    lines = [line.strip() for line in text.split("\n")]
    return [line for line in lines if line]


def _extract_fields(lines: List[str]) -> Dict[str, str]:
    labels = [
        "등록시각",
        "성함",
        "병원명 (개원예정인 경우, 개원예정)",
        "닥터팔레트를 알게된 경로",
    ]
    out: Dict[str, str] = {}

    for idx, line in enumerate(lines):
        for label in labels:
            if label in out:
                continue
            # "라벨: 값" 형태
            m = re.match(rf"^{re.escape(label)}\s*[:：]\s*(.+)$", line)
            if m:
                out[label] = m.group(1).strip()
                continue
            # "라벨" 다음 줄이 값
            if line == label and idx + 1 < len(lines):
                out[label] = lines[idx + 1].strip()
    return out


def _parse_registered_at(value: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d %H:%M", "%Y.%m.%d %H:%M"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=KST)
        except ValueError:
            continue
    return None


def _imap_connect(host: str, port: int) -> imaplib.IMAP4_SSL:
    return imaplib.IMAP4_SSL(host, port, timeout=30)


def _search_messages(
    imap: imaplib.IMAP4_SSL,
    mailbox: str,
    since_date: datetime,
    before_date: datetime,
    from_filter: str,
) -> List[bytes]:
    imap.select(mailbox)
    since_str = since_date.strftime("%d-%b-%Y")
    before_str = before_date.strftime("%d-%b-%Y")
    if from_filter:
        status, data = imap.search(None, "SINCE", since_str, "BEFORE", before_str, "FROM", from_filter)
    else:
        status, data = imap.search(None, "SINCE", since_str, "BEFORE", before_str)
    if status != "OK":
        return []
    return data[0].split()


def _fetch_message(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> Optional[Message]:
    status = ""
    data = None
    for _ in range(2):
        try:
            status, data = imap.fetch(msg_id, "(RFC822)")
        except (socket.timeout, TimeoutError, OSError, imaplib.IMAP4.error):
            continue
        if status == "OK" and data and data[0]:
            raw = data[0][1]
            return email.message_from_bytes(raw)
    if status != "OK" or not data or not data[0]:
        return None
    return None


def _fetch_subject(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> str:
    status = ""
    data = None
    for _ in range(2):
        try:
            status, data = imap.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (SUBJECT)])")
        except (socket.timeout, TimeoutError, OSError, imaplib.IMAP4.error):
            continue
        if status == "OK" and data and data[0]:
            raw = data[0][1]
            if not raw:
                return ""
            msg = email.message_from_bytes(raw)
            return _decode_subject(msg)
    if status != "OK" or not data or not data[0]:
        return ""
    return ""


def _decode_subject(msg: Message) -> str:
    subject = msg.get("Subject", "")
    decoded_parts = decode_header(subject)
    chunks: List[str] = []
    for part, enc in decoded_parts:
        if isinstance(part, bytes):
            try:
                chunks.append(part.decode(enc or "utf-8", errors="replace"))
            except LookupError:
                chunks.append(part.decode("utf-8", errors="replace"))
        else:
            chunks.append(part)
    return "".join(chunks).strip()


def _collect_inquiries(
    host: str,
    port: int,
    user: str,
    password: str,
    mailbox: str,
    since_date: datetime,
    before_date: datetime,
    from_filter: str,
    subject_filter: str,
) -> List[Dict[str, str]]:
    imap = _imap_connect(host, port)
    try:
        imap.login(user, password)
        imap._encoding = "utf-8"
        if getattr(imap, "sock", None):
            imap.sock.settimeout(_env_int("IMAP_SOCKET_TIMEOUT_SECONDS", 15))
        msg_ids = _search_messages(imap, mailbox, since_date, before_date, from_filter)
        items: List[Dict[str, str]] = []
        for msg_id in msg_ids:
            if subject_filter:
                subject = _fetch_subject(imap, msg_id)
                if subject_filter not in subject:
                    continue
            msg = _fetch_message(imap, msg_id)
            if not msg:
                continue
            html = _get_html_part(msg)
            if not html:
                continue
            lines = _html_to_lines(html)
            fields = _extract_fields(lines)
            if fields:
                items.append(fields)
        return items
    finally:
        try:
            imap.logout()
        except Exception:
            pass


def _summarize(
    items: List[Dict[str, str]],
    window_start: datetime,
    window_end: datetime,
    prev_window_start: datetime,
    prev_window_end: datetime,
) -> Tuple[int, int, Dict[str, int], List[Dict[str, str]]]:
    source_counts: Dict[str, int] = {}
    detail_items: List[Dict[str, str]] = []
    total = 0
    prev_total = 0

    for item in items:
        reg_raw = item.get("등록시각", "")
        reg_dt = _parse_registered_at(reg_raw) if reg_raw else None
        if not reg_dt:
            continue
        if prev_window_start <= reg_dt <= prev_window_end:
            prev_total += 1
            continue
        if window_start <= reg_dt <= window_end:
            total += 1
            source = item.get("닥터팔레트를 알게된 경로", "").strip() or "미기재"
            source_counts[source] = source_counts.get(source, 0) + 1
            detail_items.append(item)

    return total, prev_total, source_counts, detail_items


def _format_report(
    total: int,
    prev_total: int,
    source_counts: Dict[str, int],
    detail_items: List[Dict[str, str]],
    window_start: datetime,
    window_end: datetime,
) -> str:
    start_str = window_start.strftime("%Y-%m-%d")
    end_str = window_end.strftime("%Y-%m-%d")
    diff = total - prev_total
    if prev_total == 0:
        pct_label = "N/A"
    else:
        pct = (diff / prev_total) * 100
        pct_label = f"{pct:+.0f}%"

    if diff > 0:
        diff_label = (
            f"📈 전주 대비: {diff}건 증가 ({pct_label}) "
            f"(전전주 {prev_total}건 → 전주 {total}건)"
        )
    elif diff < 0:
        diff_label = (
            f"📉 전주 대비: {abs(diff)}건 감소 ({pct_label}) "
            f"(전전주 {prev_total}건 → 전주 {total}건)"
        )
    else:
        diff_label = f"➖ 전주 대비: 변동 없음 (0%) (전전주 {prev_total}건 → 전주 {total}건)"

    lines = [
        "*📌 [닥터팔레트 도입문의 리포트]*",
        f"*🗓️ 기간:* {start_str} ~ {end_str} (전주 월~일)",
        "",
        f"*✅ 총 문의:* {total}건",
        "",
    ]
    lines.append(diff_label)
    lines.append("")
    top_sources = sorted(source_counts.items(), key=lambda x: (-x[1], x[0]))
    if top_sources:
        top_preview = ", ".join([f"{name}({count})" for name, count in top_sources[:3]])
        lines.append(f"*🔥 Top 유입:* {top_preview}")
        lines.append("")

    lines.append("*🔎 유입 경로별*")
    if source_counts:
        max_count = max(source_counts.values())
        bar_scale = 10
        for source, count in top_sources:
            bar_len = max(1, round((count / max_count) * bar_scale)) if count > 0 else 0
            bar = "█" * bar_len
            lines.append(f"{source} | {bar} {count}")
    if not source_counts:
        lines.append("- 데이터 없음")

    if detail_items:
        lines.append("")
        lines.append("*🧾 상세 문의 (최근 7일)*")
        for item in detail_items:
            name = item.get("성함", "미기재")
            hospital = item.get("병원명 (개원예정인 경우, 개원예정)", "미기재")
            source = item.get("닥터팔레트를 알게된 경로", "미기재")
            reg_at = item.get("등록시각", "미기재")
            lines.append(f"- {name} | {hospital} | {source} | {reg_at}")
    return "\n".join(lines)


def main() -> None:
    host = _env("IMAP_HOST", "imap.gmail.com")
    port = int(_env("IMAP_PORT", "993"))
    user = _env("IMAP_USER")
    password = _env("IMAP_PASSWORD")
    mailbox = _env("IMAP_MAILBOX", "INBOX")
    from_filter = _env("IMAP_FROM_FILTER", "bonnie@medibloc.org")
    subject_filter = _env(
        "IMAP_SUBJECT_FILTER",
        "[닥터팔레트: 모두가 그리는 클라우드 EMR] [2.0] 도입문의 리뉴얼에 새 응답이 접수되었습니다.",
    )
    webhook_url = _env("SLACK_WEBHOOK_URL")
    success_state_path = _env("IMWEB_REPORT_STATE_PATH", DEFAULT_STATE_PATH)

    if not user or not password:
        raise SystemExit("IMAP_USER/IMAP_PASSWORD 환경변수가 필요합니다.")
    if not webhook_url:
        raise SystemExit("SLACK_WEBHOOK_URL 환경변수가 필요합니다.")

    now = datetime.now(tz=KST)
    # Report runs Monday 11:00; summarize previous week (Mon 00:00 ~ Sun 23:59:59)
    start_of_week = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
    window_start = start_of_week - timedelta(days=7)
    window_end = start_of_week - timedelta(seconds=1)

    prev_window_start = window_start - timedelta(days=7)
    prev_window_end = window_start - timedelta(seconds=1)

    items = _collect_inquiries(
        host=host,
        port=port,
        user=user,
        password=password,
        mailbox=mailbox,
        since_date=prev_window_start,
        before_date=window_end + timedelta(days=1),
        from_filter=from_filter,
        subject_filter=subject_filter,
    )

    total, prev_total, source_counts, detail_items = _summarize(
        items,
        window_start,
        window_end,
        prev_window_start,
        prev_window_end,
    )
    report = _format_report(total, prev_total, source_counts, detail_items, window_start, window_end)
    post_webhook(webhook_url, report)
    _write_success_state(success_state_path, now)


if __name__ == "__main__":
    main()
