import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from naver_place_review_alert import load_settings, post_webhook, stats_path


def load_review_log(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def load_review_stats(path: str) -> Dict[str, int]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def truncate_text(text: str, limit: int = 140) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def resolve_review_date(item: Dict[str, str]) -> dt.date | None:
    date_str = item.get("date", "").strip()
    if date_str:
        try:
            return dt.date.fromisoformat(date_str)
        except ValueError:
            pass
    ts = item.get("ts", "").strip()
    if ts:
        try:
            return dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").date()
        except ValueError:
            pass
    return None


def filter_last_days(reviews: List[Dict[str, str]], days: int) -> List[Dict[str, str]]:
    today = dt.date.today()
    cutoff = today - dt.timedelta(days=days - 1)
    filtered: List[Dict[str, str]] = []
    for item in reviews:
        review_date = resolve_review_date(item)
        if review_date is None:
            continue
        if review_date >= cutoff:
            filtered.append(item)
    return filtered


def classify_sentiment(text: str) -> str:
    normalized = text.replace(" ", "")
    positive_phrases = [
        "안아프",
        "하나도안아프",
        "아프지말라고",
        "통증없",
        "통증적",
    ]
    if any(phrase in normalized for phrase in positive_phrases):
        return "positive"

    negative_keywords = [
        "불친절",
        "별로",
        "최악",
        "실망",
        "아프",
        "비추",
        "후회",
        "문제",
        "불만",
        "기다림",
        "대기김",
        "효과없",
        "재방문안",
    ]
    positive_keywords = [
        "친절",
        "만족",
        "좋아",
        "추천",
        "깔끔",
        "효과",
        "정착",
        "최고",
        "세심",
        "꼼꼼",
        "편해",
    ]
    if any(keyword in normalized for keyword in negative_keywords):
        return "negative"
    if any(keyword in normalized for keyword in positive_keywords):
        return "positive"
    return "neutral"


def weekly_trend_message(all_reviews: List[Dict[str, str]], current_start: dt.date) -> str:
    weekly_counts: Dict[str, int] = defaultdict(int)
    for item in all_reviews:
        review_date = resolve_review_date(item)
        if review_date is None:
            continue
        monday = review_date - dt.timedelta(days=review_date.weekday())
        weekly_counts[monday.isoformat()] += 1

    previous_weeks: List[int] = []
    for offset in range(1, 5):
        week_start = current_start - dt.timedelta(days=7 * offset)
        count = weekly_counts.get(week_start.isoformat())
        if count is not None:
            previous_weeks.append(count)

    if len(previous_weeks) < 4:
        return "4주 평균 비교를 위한 과거 주간 데이터가 아직 부족합니다."

    current_count = weekly_counts.get(current_start.isoformat(), 0)
    baseline = sum(previous_weeks) / len(previous_weeks)
    if current_count > baseline:
        return f"직전 4주 평균 대비 신규 리뷰가 {current_count - baseline:.1f}건 많습니다."
    if current_count < baseline:
        return f"직전 4주 평균 대비 신규 리뷰가 {baseline - current_count:.1f}건 적습니다."
    return "직전 4주 평균과 비슷한 수준입니다."


def build_weekly_payload(
    weekly_reviews: List[Dict[str, str]],
    all_reviews: List[Dict[str, str]],
    total_review_count: int,
    start_date: dt.date,
    end_date: dt.date,
) -> Dict[str, Any]:
    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    for item in weekly_reviews:
        sentiment_counts[classify_sentiment(item.get("text", ""))] += 1

    if sentiment_counts["negative"] > 0:
        summary = (
            f"이번 주 신규 리뷰 {len(weekly_reviews)}건 중 "
            f"부정 반응 {sentiment_counts['negative']}건이 확인되었습니다."
        )
    elif sentiment_counts["neutral"] > 0:
        summary = (
            f"이번 주 신규 리뷰 {len(weekly_reviews)}건은 "
            "대체로 긍정적이지만 일부 중립 반응이 포함되었습니다."
        )
    else:
        summary = f"이번 주 신규 리뷰 {len(weekly_reviews)}건이 모두 긍정 반응입니다."

    trend = weekly_trend_message(all_reviews, start_date)
    latest_reviews = sorted(
        weekly_reviews,
        key=lambda item: (
            item.get("date", ""),
            item.get("ts", ""),
        ),
        reverse=True,
    )[:3]
    negative_reviews = [
        item for item in weekly_reviews if classify_sentiment(item.get("text", "")) == "negative"
    ]
    overview_lines = [
        f"*기간*  {start_date.isoformat()} ~ {end_date.isoformat()}",
        f"*신규 리뷰*  {len(weekly_reviews)}건",
        (
            f"*누적 리뷰*  {total_review_count}건"
            if total_review_count > 0
            else "*누적 리뷰*  집계 대기중"
        ),
        (
            "*감성 분포*  "
            f"😊 {sentiment_counts['positive']} | "
            f"😐 {sentiment_counts['neutral']} | "
            f"😟 {sentiment_counts['negative']}"
        ),
        f"*요약*  {summary}",
        f"*추세 판단*  {trend}",
    ]

    review_lines: List[str] = []
    if latest_reviews:
        for idx, item in enumerate(latest_reviews, start=1):
            sentiment = classify_sentiment(item.get("text", ""))
            if sentiment == "positive":
                marker = "😊"
                label = "긍정"
            elif sentiment == "negative":
                marker = "😟"
                label = "부정"
            else:
                marker = "😐"
                label = "중립"
            review_date = item.get("date") or "-"
            review_lines.append(
                f"*{idx}. {marker} {label} | {review_date}*\n{truncate_text(item.get('text', ''))}"
            )
    else:
        review_lines.append("이번 주 수집된 리뷰가 없습니다.")

    negative_lines: List[str] = []
    if negative_reviews:
        negative_lines.append(f"⚠️ 이번 주 수집된 부정 후기는 {len(negative_reviews)}건입니다.")
        for item in negative_reviews[:5]:
            review_date = item.get("date") or "-"
            negative_lines.append(f"• {review_date} | {truncate_text(item.get('text', ''), limit=100)}")
    else:
        negative_lines.append("✅ 이번 주 수집된 부정 후기는 없습니다.")

    fallback_text = (
        f"네이버 플레이스 주간 리포트 | 기간: {start_date.isoformat()} ~ {end_date.isoformat()} | "
        f"신규 리뷰: {len(weekly_reviews)}건 | 누적 리뷰: "
        f"{total_review_count if total_review_count > 0 else '집계 대기중'}"
    )

    return {
        "text": fallback_text,
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "네이버 플레이스 주간 리포트"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(overview_lines)},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*📝 주요 리뷰 요약*"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n\n".join(review_lines)},
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*🚨 부정 후기 체크*"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(negative_lines)},
            },
        ],
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    settings = load_settings()
    archive_path = os.getenv(
        "REVIEW_ARCHIVE_PATH",
        str(project_root / "data" / "naver_place_review_archive.jsonl"),
    )
    weekly_hour = int(os.getenv("WEEKLY_HOUR", "11"))
    weekly_minute = int(os.getenv("WEEKLY_MINUTE", "30"))
    force_weekly = os.getenv("FORCE_WEEKLY", "").strip().lower() in ("1", "true", "yes")
    state_path = os.getenv(
        "WEEKLY_STATE_PATH",
        str(project_root / "data" / "weekly_review_last_sent.txt"),
    )

    now = dt.datetime.now()
    if not force_weekly:
        if now.weekday() != 0:
            return
        if (now.hour, now.minute) < (weekly_hour, weekly_minute):
            return
        try:
            if os.path.exists(state_path):
                with open(state_path, "r", encoding="utf-8") as f:
                    last_sent = f.read().strip()
                if last_sent == now.strftime("%Y-%m-%d"):
                    return
        except OSError:
            pass

    all_reviews = load_review_log(archive_path)
    weekly_reviews = filter_last_days(all_reviews, days=7)
    today = dt.date.today()
    start_date = today - dt.timedelta(days=6)
    stats = load_review_stats(stats_path(project_root))
    total_review_count = int(stats.get("total_review_count") or 0)

    if not settings.review_slack_webhook_url:
        raise RuntimeError(
            "SLACK_WEBHOOK_URL_REVIEW is missing. Set it in .env "
            "(or keep SLACK_WEBHOOK_URL as a fallback)."
        )

    payload = build_weekly_payload(
        weekly_reviews=weekly_reviews,
        all_reviews=all_reviews,
        total_review_count=total_review_count,
        start_date=start_date,
        end_date=today,
    )
    post_webhook(settings.review_slack_webhook_url, payload)
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        f.write(today.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    main()
