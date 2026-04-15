import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

try:
    from .naver_place_keyword_config import default_config_path, keyword_meta_map, load_keyword_config
    from .naver_place_review_alert import post_webhook
except ImportError:
    from naver_place_keyword_config import default_config_path, keyword_meta_map, load_keyword_config
    from naver_place_review_alert import post_webhook


def load_rank_logs(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def filter_last_days(rows: List[dict], days: int) -> List[dict]:
    today = dt.date.today()
    cutoff = today - dt.timedelta(days=days - 1)
    filtered: List[dict] = []
    for row in rows:
        date_str = row.get("date", "")
        if not date_str:
            continue
        try:
            row_date = dt.date.fromisoformat(date_str)
        except ValueError:
            continue
        if row_date >= cutoff:
            filtered.append(row)
    return filtered


def summarize_keyword(rows: List[dict]) -> Dict[str, Optional[float]]:
    ranks = [r.get("rank") for r in rows if isinstance(r.get("rank"), int)]
    misses = len([r for r in rows if r.get("rank") is None])
    if not ranks:
        return {
            "avg": None,
            "best": None,
            "worst": None,
            "latest": None,
            "first": None,
            "misses": misses,
            "samples": len(rows),
        }
    avg = sum(ranks) / len(ranks)
    best = min(ranks)
    worst = max(ranks)
    latest = rows[-1].get("rank") if rows else None
    first = rows[0].get("rank") if rows else None
    return {
        "avg": avg,
        "best": best,
        "worst": worst,
        "latest": latest,
        "first": first,
        "misses": misses,
        "samples": len(rows),
    }


def trend_arrow(first: Optional[int], latest: Optional[int]) -> str:
    if first is None or latest is None:
        return "—"
    if latest < first:
        return "⬆️"
    if latest > first:
        return "⬇️"
    return "➡️"


def describe_strength(keyword: str, stats: Dict[str, Optional[float]]) -> Optional[str]:
    latest = stats.get("latest")
    avg = stats.get("avg")
    best = stats.get("best")
    if not isinstance(latest, int) and not isinstance(avg, float):
        return None
    if isinstance(latest, int) and latest <= 10:
        return f"{keyword}: 최근 {latest}위로 TOP10 안착"
    if isinstance(avg, float) and avg <= 15:
        return f"{keyword}: 주간 평균 {avg:.1f}위로 안정적 노출"
    if isinstance(best, int) and best <= 5:
        return f"{keyword}: 최고 {best}위까지 도달"
    return None


def describe_weakness(keyword: str, stats: Dict[str, Optional[float]]) -> Optional[str]:
    misses = int(stats.get("misses") or 0)
    samples = int(stats.get("samples") or 0)
    latest = stats.get("latest")
    avg = stats.get("avg")
    if samples and misses == samples:
        return f"{keyword}: 이번 주 내내 순위 미확인"
    if misses >= 2:
        return f"{keyword}: 순위 누락 {misses}회 발생"
    if isinstance(latest, int) and latest >= 30:
        return f"{keyword}: 최근 {latest}위로 노출 약세"
    if isinstance(avg, float) and avg >= 20:
        return f"{keyword}: 주간 평균 {avg:.1f}위로 개선 필요"
    return None


def build_category_block(
    category: str,
    keyword_stats: Dict[str, Dict[str, Optional[float]]],
    keyword_labels: Dict[str, str],
) -> List[str]:
    lines = [f"📂 {category}"]
    strengths: List[str] = []
    weaknesses: List[str] = []

    ordered = sorted(
        keyword_stats.items(),
        key=lambda item: (
            item[1].get("latest") if isinstance(item[1].get("latest"), int) else 10**6,
            item[0],
        ),
    )
    for keyword, stats in ordered:
        label = keyword_labels.get(keyword, keyword)
        arrow = trend_arrow(
            stats.get("first") if isinstance(stats.get("first"), int) else None,
            stats.get("latest") if isinstance(stats.get("latest"), int) else None,
        )
        latest = stats.get("latest")
        avg = stats.get("avg")
        miss = int(stats.get("misses") or 0)
        lines.append(
            f"- {label}: 최근 {latest if latest is not None else '미확인'}위 {arrow} / "
            f"평균 {f'{avg:.1f}위' if avg is not None else 'N/A'} / 누락 {miss}회"
        )

        strength = describe_strength(label, stats)
        if strength:
            strengths.append(strength)
        weakness = describe_weakness(label, stats)
        if weakness:
            weaknesses.append(weakness)

    if strengths:
        lines.append("잘한 점")
        for item in strengths[:3]:
            lines.append(f"- {item}")
    if weaknesses:
        lines.append("약한 점")
        for item in weaknesses[:3]:
            lines.append(f"- {item}")
    lines.append("────────────────────────")
    return lines


def build_message(
    summary: Dict[str, Dict[str, Dict[str, Optional[float]]]],
    start_date: str,
    end_date: str,
    active_keywords: List[dict],
    target_name: str,
) -> str:
    lines = [
        "📈 네이버 플레이스 주간 키워드 리포트",
        "────────────────────────",
        f"📅 기간: {start_date}~{end_date}",
        f"🏥 대상: {target_name}",
        "📱 기준: 모바일 / 광고 포함 / 비로그인",
    ]

    if active_keywords:
        grouped_active: Dict[str, List[str]] = defaultdict(list)
        for item in active_keywords:
            grouped_active[item.get("category", "기타")].append(item.get("label", item["keyword"]))
        lines.append("🗂️ 이번 주 활성 키워드")
        for category, labels in grouped_active.items():
            lines.append(f"- {category}: {', '.join(labels)}")
    lines.append("────────────────────────")

    keyword_labels = {item["keyword"]: item.get("label", item["keyword"]) for item in active_keywords}
    for category, stats_by_keyword in summary.items():
        lines.extend(build_category_block(category, stats_by_keyword, keyword_labels))

    return "\n".join(lines).rstrip()


def write_report_output(path: str, message: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(message)
        f.write("\n")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    weekly_hour = int(os.getenv("WEEKLY_HOUR", "11"))
    weekly_minute = int(os.getenv("WEEKLY_MINUTE", "30"))
    force_weekly = os.getenv("FORCE_WEEKLY", "").strip().lower() in ("1", "true", "yes")
    state_path = os.getenv(
        "WEEKLY_STATE_PATH",
        str(project_root / "data" / "weekly_rank_last_sent.txt"),
    )
    output_path = os.getenv(
        "WEEKLY_RANK_REPORT_OUTPUT_PATH",
        str(project_root / "out" / "naver_place_weekly_rank_report.txt"),
    ).strip()
    target_name = os.getenv("TARGET_PLACE_NAME", "세예의원").strip()

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
        except Exception:
            pass

    log_path = os.getenv("RANK_LOG_PATH", "data/naver_place_rank_log.jsonl").strip()
    slack_url = (
        os.getenv("SLACK_WEBHOOK_URL_RANK", "").strip()
        or os.getenv("SLACK_WEBHOOK_URL", "").strip()
    )
    use_keyword_config = os.getenv("RANK_USE_KEYWORD_CONFIG", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    config_path = Path(
        os.getenv("RANK_KEYWORD_CONFIG_PATH", str(default_config_path(project_root))).strip()
    )
    if use_keyword_config:
        keyword_config = load_keyword_config(config_path)
        active_keywords = keyword_meta_map(keyword_config)
    else:
        active_keywords = {
            keyword: {
                "keyword": keyword,
                "category": "지역 키워드",
                "label": keyword,
                "intent": "",
                "note": "",
            }
            for keyword in [
                q.strip()
                for q in os.getenv("RANK_KEYWORDS", "신논현피부과,강남역피부과").split(",")
                if q.strip()
            ]
        }

    rows = load_rank_logs(log_path)
    recent = filter_last_days(rows, days=7)
    recent = [row for row in recent if row.get("keyword") in active_keywords]

    today = dt.date.today()
    start = today - dt.timedelta(days=6)
    start_label = start.strftime("%y/%m/%d")
    end_label = today.strftime("%y/%m/%d")

    if not recent:
        message = (
            "📈 네이버 플레이스 주간 키워드 리포트\n"
            "────────────────────────\n"
            f"📅 기간: {start_label}~{end_label}\n"
            f"🏥 대상: {target_name}\n"
            "기록된 순위 데이터가 없습니다."
        )
        write_report_output(output_path, message)
        print(message)
        if slack_url:
            post_webhook(slack_url, message)
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            with open(state_path, "w", encoding="utf-8") as f:
                f.write(today.strftime("%Y-%m-%d"))
        return

    by_category: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in recent:
        keyword = row.get("keyword", "unknown")
        config_meta = active_keywords.get(keyword, {})
        category = row.get("category") or config_meta.get("category") or "기타"
        by_category[category][keyword].append(row)

    summary: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
    for category, by_keyword in by_category.items():
        summary[category] = {}
        for keyword, keyword_rows in by_keyword.items():
            rows_sorted = sorted(
                keyword_rows, key=lambda r: (r.get("date", ""), r.get("time", ""))
            )
            summary[category][keyword] = summarize_keyword(rows_sorted)

    active_entries = list(active_keywords.values())
    message = build_message(summary, start_label, end_label, active_entries, target_name)
    write_report_output(output_path, message)
    print(message)
    if slack_url:
        post_webhook(slack_url, message)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as f:
            f.write(today.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    main()
