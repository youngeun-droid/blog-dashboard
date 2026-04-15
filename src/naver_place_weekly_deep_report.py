import datetime as dt
import json
import os
import re
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

try:
    from .naver_place_keyword_config import default_config_path, load_keyword_config, resolve_active_keywords
    from .naver_place_review_alert import post_webhook
    from .sources.naver_datalab import NaverDataLabConfig, fetch_search_trends, summarize_trend
    from .naver_place_weekly_rank_report import load_rank_logs
except ImportError:
    from naver_place_keyword_config import default_config_path, load_keyword_config, resolve_active_keywords
    from naver_place_review_alert import post_webhook
    from sources.naver_datalab import NaverDataLabConfig, fetch_search_trends, summarize_trend
    from naver_place_weekly_rank_report import load_rank_logs


MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
    "Mobile/15E148 Safari/604.1"
)

APOLLO_PATTERN = re.compile(r"window\.__APOLLO_STATE__\s*=\s*(\{.*?\});", re.S)


def build_search_url(query: str, x: float, y: float, start: int, display: int) -> str:
    from urllib.parse import quote_plus

    q = quote_plus(query)
    return (
        "https://m.place.naver.com/place/list"
        f"?query={q}"
        f"&x={x}&y={y}"
        f"&start={start}&display={display}"
        "&deviceType=mobile&sortingOrder=precision&level=top"
    )


def parse_apollo_state(html: str) -> dict:
    match = APOLLO_PATTERN.search(html)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def parse_int(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    digits = re.sub(r"[^0-9]", "", str(value))
    return int(digits) if digits else None


def parse_distance_meters(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    text = str(value).strip().lower().replace(" ", "")
    if text.endswith("km"):
        try:
            return int(round(float(text[:-2]) * 1000))
        except ValueError:
            return None
    if text.endswith("m"):
        try:
            return int(round(float(text[:-1])))
        except ValueError:
            return None
    return parse_int(text)


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", "", value or "").lower()


def normalize_keyword(value: str) -> str:
    return re.sub(r"\s+", "", value or "").lower()


def clean_optional_text(value) -> Optional[str]:
    if value in (None, "", "None", "null"):
        return None
    text = str(value).strip()
    return text or None


def format_delta(current: Optional[int], previous: Optional[int]) -> str:
    if current is None and previous is None:
        return "변화 없음"
    if current is None:
        return "이번 주 미노출"
    if previous is None:
        return f"전주 데이터 없음 (이번 주 {current}위)"
    diff = previous - current
    if diff > 0:
        return f"{diff}계단 상승"
    if diff < 0:
        return f"{abs(diff)}계단 하락"
    return "유지"


def mean_metric(rows: List[dict], key: str) -> Optional[float]:
    values = [row[key] for row in rows if isinstance(row.get(key), (int, float))]
    if not values:
        return None
    return sum(values) / len(values)


def booking_rate(rows: List[dict]) -> float:
    if not rows:
        return 0.0
    total = len(rows)
    booked = len([row for row in rows if row.get("has_booking") is True])
    return booked / total if total else 0.0


def fetch_places(query: str, x: float, y: float, display: int) -> List[dict]:
    url = build_search_url(query, x, y, start=1, display=display)
    response = requests.get(
        url,
        headers={"User-Agent": MOBILE_UA},
        timeout=20,
    )
    response.raise_for_status()
    response.encoding = "utf-8"

    apollo = parse_apollo_state(response.text)
    rows: List[dict] = []
    for key, payload in apollo.items():
        if not key.startswith("PlaceSummary:") or not isinstance(payload, dict):
            continue
        name = str(payload.get("name", "")).strip()
        if not name:
            continue
        rows.append(
            {
                "place_id": str(payload.get("id", "")).strip() or None,
                "name": name,
                "category": str(payload.get("category", "")).strip(),
                "distance_text": str(payload.get("distance", "")).strip() or None,
                "distance_m": parse_distance_meters(payload.get("distance")),
                "visitor_review_count": parse_int(payload.get("visitorReviewCount")),
                "blog_review_count": parse_int(payload.get("blogCafeReviewCount")),
                "image_count": parse_int(payload.get("imageCount")),
                "has_booking": bool(payload.get("hasBooking")),
                "booking_review_count": parse_int(payload.get("bookingReviewCount")),
                "road_address": str(payload.get("roadAddress", "")).strip(),
                "full_address": str(payload.get("fullAddress", "")).strip(),
                "phone": clean_optional_text(payload.get("phone")),
                "x": clean_optional_text(payload.get("x")),
                "y": clean_optional_text(payload.get("y")),
                "is_ad": None,
            }
        )

    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def select_keywords(keyword_entries: List[dict], procedure_limit: int) -> List[dict]:
    regional = [item for item in keyword_entries if item.get("category") == "지역 키워드"]
    procedures = [item for item in keyword_entries if item.get("category") == "시술 키워드"]
    selected: List[dict] = []
    seen = set()

    procedure_items = procedures if procedure_limit <= 0 else procedures[:procedure_limit]

    for item in regional + procedure_items:
        keyword = item["keyword"]
        if keyword in seen:
            continue
        seen.add(keyword)
        selected.append(item)
    return selected


def match_target(row: dict, target_name: str, target_place_id: str) -> bool:
    if target_place_id and row.get("place_id") == target_place_id:
        return True
    return normalize_name(target_name) in normalize_name(row.get("name", ""))


def latest_previous_rank(rows: List[dict], keyword: str, today: dt.date) -> Optional[int]:
    start_prev = today - dt.timedelta(days=14)
    end_prev = today - dt.timedelta(days=7)
    matches = [
        row for row in rows
        if row.get("keyword") == keyword
        and row.get("date")
    ]
    filtered = []
    for row in matches:
        try:
            row_date = dt.date.fromisoformat(str(row.get("date")))
        except ValueError:
            continue
        if start_prev <= row_date < end_prev:
            filtered.append(row)
    filtered.sort(key=lambda row: (row.get("date", ""), row.get("time", "")))
    latest = filtered[-1] if filtered else None
    return latest.get("rank") if latest else None


def load_keyword_metrics(path: str) -> Dict[str, List[dict]]:
    if not os.path.exists(path):
        return {}
    bucket: Dict[str, List[dict]] = defaultdict(list)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keyword = str(row.get("keyword", "")).strip()
            if not keyword:
                continue
            saturation_raw = row.get("saturation")
            try:
                saturation = float(saturation_raw) if saturation_raw not in (None, "", "None") else None
            except ValueError:
                saturation = None
            bucket[normalize_keyword(keyword)].append(
                {
                    "keyword": keyword,
                    "search_volume": parse_int(row.get("search_volume")),
                    "saturation": saturation,
                    "updated_at": str(row.get("updated_at", "")).strip(),
                }
            )
    for values in bucket.values():
        values.sort(key=lambda item: item.get("updated_at", ""))
    return bucket


def resolve_metric(metrics_map: Dict[str, List[dict]], keyword: str) -> Optional[dict]:
    values = metrics_map.get(normalize_keyword(keyword), [])
    if values:
        return values[-1]
    return None


def metric_trend_text(metrics_map: Dict[str, List[dict]], keyword: str) -> Optional[str]:
    values = metrics_map.get(normalize_keyword(keyword), [])
    if len(values) < 2:
        return None
    prev = values[-2].get("search_volume")
    curr = values[-1].get("search_volume")
    if not isinstance(prev, int) or not isinstance(curr, int):
        return None
    diff = curr - prev
    if diff > 0:
        return f"최근 검색량 +{diff}"
    if diff < 0:
        return f"최근 검색량 {diff}"
    return "최근 검색량 변화 없음"


def resolve_datalab_summary(trend_map: Dict[str, List[dict]], keyword: str) -> Optional[dict]:
    points = trend_map.get(keyword, [])
    if not points:
        return None
    return summarize_trend(points)


def compare_against_competitors(target: dict, competitors: List[dict]) -> Dict[str, List[str]]:
    strengths: List[str] = []
    weaknesses: List[str] = []
    tags: List[str] = []

    avg_distance = mean_metric(competitors, "distance_m")
    avg_blog = mean_metric(competitors, "blog_review_count")
    avg_visitor = mean_metric(competitors, "visitor_review_count")
    avg_image = mean_metric(competitors, "image_count")
    avg_booking_rate = booking_rate(competitors)

    target_distance = target.get("distance_m")
    if isinstance(target_distance, int) and isinstance(avg_distance, float):
        if target_distance > avg_distance * 1.25 and target_distance - avg_distance >= 100:
            weaknesses.append(
                f"거리 열세: 상위 경쟁사 평균 {avg_distance:.0f}m 대비 {target_distance}m"
            )
            tags.append("distance")
        elif target_distance < avg_distance * 0.8:
            strengths.append(
                f"거리 강점: 상위 경쟁사 평균 {avg_distance:.0f}m 대비 {target_distance}m"
            )

    target_blog = target.get("blog_review_count")
    if isinstance(target_blog, int) and isinstance(avg_blog, float):
        if target_blog < avg_blog * 0.7:
            weaknesses.append(
                f"블로그 리뷰 약세: 상위 경쟁사 평균 {avg_blog:.0f}건 대비 {target_blog}건"
            )
            tags.append("blog")
        elif target_blog > avg_blog * 1.2:
            strengths.append(
                f"블로그 리뷰 강점: 상위 경쟁사 평균 {avg_blog:.0f}건 대비 {target_blog}건"
            )

    target_visitor = target.get("visitor_review_count")
    if isinstance(target_visitor, int) and isinstance(avg_visitor, float):
        if target_visitor < avg_visitor * 0.7:
            weaknesses.append(
                f"방문자 리뷰 약세: 상위 경쟁사 평균 {avg_visitor:.0f}건 대비 {target_visitor}건"
            )
            tags.append("visitor")
        elif target_visitor > avg_visitor * 1.2:
            strengths.append(
                f"방문자 리뷰 강점: 상위 경쟁사 평균 {avg_visitor:.0f}건 대비 {target_visitor}건"
            )

    target_image = target.get("image_count")
    if isinstance(target_image, int) and isinstance(avg_image, float):
        if target_image < avg_image * 0.7:
            weaknesses.append(
                f"이미지 약세: 상위 경쟁사 평균 {avg_image:.0f}장 대비 {target_image}장"
            )
            tags.append("image")
        elif target_image > avg_image * 1.2:
            strengths.append(
                f"이미지 강점: 상위 경쟁사 평균 {avg_image:.0f}장 대비 {target_image}장"
            )

    if avg_booking_rate >= 0.6 and target.get("has_booking") is not True:
        weaknesses.append(
            f"예약 기능 약세: 상위 경쟁사 예약 활성 비율 {avg_booking_rate:.0%}, 우리 병원 예약 비활성"
        )
        tags.append("booking")
    elif avg_booking_rate < 1.0 and target.get("has_booking") is True:
        strengths.append("예약 기능 강점: 상위권과 비교해 전환 장치 유지")

    return {
        "strengths": strengths[:3],
        "weaknesses": weaknesses[:3],
        "weakness_tags": tags,
    }


def keyword_status_text(current_rank: Optional[int]) -> str:
    if current_rank is None:
        return "미노출 키워드"
    if current_rank <= 5:
        return "강한 키워드"
    if current_rank <= 15:
        return "유지 키워드"
    if current_rank <= 30:
        return "보완 필요 키워드"
    return "우선순위 낮은 키워드"


def top_names(rows: List[dict], limit: int = 3) -> str:
    names = [row.get("name", "") for row in rows[:limit] if row.get("name")]
    return ", ".join(names) if names else "상위 병원"


def build_keyword_narrative(
    label: str,
    current_rank: Optional[int],
    target: Optional[dict],
    competitors: List[dict],
    strengths: List[str],
    weaknesses: List[str],
) -> List[str]:
    avg_distance = mean_metric(competitors, "distance_m")
    avg_blog = mean_metric(competitors, "blog_review_count")
    avg_visitor = mean_metric(competitors, "visitor_review_count")
    lines: List[str] = []
    leaders = top_names(competitors)

    if current_rank is None:
        parts = [f"{label} 키워드는 현재 미노출입니다."]
        if leaders:
            parts.append(f"상위권은 {leaders} 중심입니다.")
        if avg_distance is not None:
            parts.append(f"상위 경쟁사는 평균 {avg_distance:.0f}m 거리권에 있습니다.")
        if avg_blog is not None:
            parts.append(f"블로그 리뷰는 평균 {avg_blog:.0f}건 수준입니다.")
        lines.append(" ".join(parts))
        return lines

    if current_rank <= 5:
        parts = [f"{label} 키워드는 현재 상위 노출이 잘 되는 강한 키워드입니다."]
        if leaders:
            parts.append(f"상위권은 {leaders} 중심입니다.")
        if strengths:
            parts.append(f"현재 우위 지표는 {strengths[0]}입니다.")
        if weaknesses:
            parts.append(f"다만 더 끌어올리려면 {weaknesses[0]}를 보완해야 합니다.")
        lines.append(" ".join(parts))
        return lines

    if current_rank <= 15:
        parts = [f"{label} 키워드는 현재 노출은 확보됐지만 상위권 고정은 더 필요합니다."]
        if leaders:
            parts.append(f"상위권은 {leaders} 중심입니다.")
        if weaknesses:
            parts.append(f"가장 먼저 볼 차이는 {weaknesses[0]}입니다.")
        if strengths:
            parts.append(f"반면 {strengths[0]}은 유지 강점입니다.")
        lines.append(" ".join(parts))
        return lines

    parts = [f"{label} 키워드는 현재 보완이 필요한 상태입니다."]
    if leaders:
        parts.append(f"상위권은 {leaders} 중심입니다.")
    if weaknesses:
        parts.append(f"상위권과의 가장 큰 차이는 {weaknesses[0]}입니다.")
    if strengths:
        parts.append(f"다만 {strengths[0]}은 유지되는 강점입니다.")
    elif avg_blog is not None and target and isinstance(target.get("blog_review_count"), int):
        parts.append(f"블로그 리뷰는 상위 경쟁사 평균 {avg_blog:.0f}건 대비 {target['blog_review_count']}건입니다.")
    lines.append(" ".join(parts))
    return lines


def build_short_insight(item: dict) -> str:
    datalab = item.get("datalab_summary") or {}
    trend_summary = datalab.get("summary")
    if item["current_rank"] is None:
        leaders = top_names(item.get("competitors", []))
        pieces = [f"미노출, 상위권은 {leaders} 중심"]
        if trend_summary:
            pieces.append(trend_summary)
        return " / ".join(pieces)
    if item["current_rank"] <= 5:
        if trend_summary and "상승" in trend_summary:
            return f"상위권 유지, 최근 추세는 상승"
        if item["strengths"]:
            return f"상위권 유지, 강점은 {item['strengths'][0]}"
        return "상위권 유지"
    if item["weaknesses"]:
        gap = item.get("blog_gap_text")
        if gap and "블로그" in item["weaknesses"][0]:
            return f"보완 포인트는 {item['weaknesses'][0]} ({gap})"
        if trend_summary and "상승" in trend_summary:
            return f"추세는 상승, 보완 포인트는 {item['weaknesses'][0]}"
        return f"보완 포인트는 {item['weaknesses'][0]}"
    if item["strengths"]:
        return f"유지 강점은 {item['strengths'][0]}"
    if trend_summary:
        return trend_summary
    return "특이사항 없음"


def summarize_keyword_group(items: List[dict], include_rank: bool = True, limit: int = 5) -> str:
    parts: List[str] = []
    for item in items[:limit]:
        if include_rank:
            rank_text = f"{item['current_rank']}위" if item["current_rank"] is not None else "미노출"
            parts.append(f"{item['label']} {rank_text}")
        else:
            parts.append(item["label"])
    if not parts:
        return "-"
    extra = len(items) - len(parts)
    text = ", ".join(parts)
    if extra > 0:
        text += f" 외 {extra}개"
    return text


def build_keyword_action(label: str, current_rank: Optional[int], weaknesses: List[str], strengths: List[str]) -> str:
    if current_rank is None:
        return f"{label}: 현재 미노출이므로 해당 키워드는 신규 유입 확보용 보완 키워드로 보고 콘텐츠와 리뷰 자산부터 쌓는 게 우선입니다."
    if current_rank <= 5 and not weaknesses:
        if strengths:
            return f"{label}: 이미 상위권이므로 {strengths[0]}을 유지하는 방어 운영이 우선입니다."
        return f"{label}: 이미 상위권이므로 리뷰와 예약 전환 품질을 유지하는 방어 운영이 우선입니다."
    if current_rank <= 5 and weaknesses:
        return f"{label}: 현재 상위권이지만 {weaknesses[0]}를 줄이면 TOP 3 경쟁이 가능합니다."
    if weaknesses:
        if strengths:
            return f"{label}: 이번 주에는 {weaknesses[0]}가 핵심 보완 포인트이고, {strengths[0]}은 유지해야 할 강점입니다."
        return f"{label}: 이번 주에는 {weaknesses[0]}가 핵심 보완 포인트입니다."
    if strengths:
        return f"{label}: {strengths[0]}이 유지되고 있어 강점 유지 운영이 적합합니다."
    return f"{label}: 이번 주 수집 기준으로 추가 확인이 필요한 키워드입니다."


def build_blog_gap_text(target: Optional[dict], competitors: List[dict]) -> Optional[str]:
    if not target:
        return None
    avg_blog = mean_metric(competitors, "blog_review_count")
    target_blog = target.get("blog_review_count")
    if not isinstance(avg_blog, float) or not isinstance(target_blog, int):
        return None
    gap = int(round(avg_blog - target_blog))
    if gap <= 0:
        return None
    return f"블로그 리뷰는 상위 경쟁사 평균 대비 약 {gap:,}건 적습니다."


def future_gap_insight(item: dict) -> Optional[str]:
    label = item.get("label", "")
    current_rank = item.get("current_rank")
    datalab = item.get("datalab_summary") or {}
    summary = datalab.get("summary")
    direction = datalab.get("direction")
    weaknesses = item.get("weaknesses") or []

    if current_rank is None and direction in {"rising", "flat"}:
        reason = weaknesses[0] if weaknesses else "현재 미노출 상태"
        return f"{label}: 추세는 {summary}인데 현재 미노출이라 앞으로 보완이 필요합니다. 핵심 격차는 {reason}입니다."

    if isinstance(current_rank, int) and current_rank <= 5 and direction == "falling":
        delta = datalab.get("delta")
        delta_text = f" ({delta:+.2f})" if isinstance(delta, (int, float)) else ""
        return f"{label}: 데이터랩 트렌드 추세는 {summary}{delta_text}이지만 현재 상위 노출 중이라 신규 확대보다 순위 방어가 우선입니다."

    if isinstance(current_rank, int) and current_rank >= 16:
        reason = weaknesses[0] if weaknesses else "상위권 대비 지표 열세"
        return f"{label}: 현재 하위 노출 구간이라 앞으로 보완이 필요합니다. 핵심 격차는 {reason}입니다."

    return None


def analyze_keyword(
    entry: dict,
    rows: List[dict],
    target_name: str,
    target_place_id: str,
    competitor_limit: int,
    previous_rank: Optional[int],
    metric: Optional[dict],
    metric_trend: Optional[str],
    datalab_summary: Optional[dict],
) -> dict:
    target = next((row for row in rows if match_target(row, target_name, target_place_id)), None)
    competitors = [row for row in rows if not match_target(row, target_name, target_place_id)][:competitor_limit]
    compare = compare_against_competitors(target, competitors) if target and competitors else {
        "strengths": [],
        "weaknesses": [],
        "weakness_tags": [],
    }
    narratives = build_keyword_narrative(
        label=entry.get("label", entry["keyword"]),
        current_rank=target.get("rank") if target else None,
        target=target,
        competitors=competitors,
        strengths=compare["strengths"],
        weaknesses=compare["weaknesses"],
    )
    action = build_keyword_action(
        label=entry.get("label", entry["keyword"]),
        current_rank=target.get("rank") if target else None,
        weaknesses=compare["weaknesses"],
        strengths=compare["strengths"],
    )
    blog_gap_text = build_blog_gap_text(target, competitors)

    display_rows = rows[:competitor_limit]
    if target and all(row.get("rank") != target.get("rank") for row in display_rows):
        display_rows = display_rows + [target]

    result = {
        "keyword": entry["keyword"],
        "label": entry.get("label", entry["keyword"]),
        "category": entry.get("category", "기타"),
        "intent": entry.get("intent", ""),
        "current_rank": target.get("rank") if target else None,
        "previous_rank": previous_rank,
        "rank_delta_text": format_delta(target.get("rank") if target else None, previous_rank),
        "target_found": bool(target),
        "target": target,
        "competitors": competitors,
        "display_rows": display_rows,
        "strengths": compare["strengths"],
        "weaknesses": compare["weaknesses"],
        "weakness_tags": compare["weakness_tags"],
        "status_text": keyword_status_text(target.get("rank") if target else None),
        "narratives": narratives,
        "short_insight": build_short_insight(
            {
                "current_rank": target.get("rank") if target else None,
                "competitors": competitors,
                "strengths": compare["strengths"],
                "weaknesses": compare["weaknesses"],
                "metric": metric,
                "metric_trend": metric_trend,
                "blog_gap_text": blog_gap_text,
                "datalab_summary": datalab_summary,
            }
        ),
        "action": action,
        "metric": metric,
        "metric_trend": metric_trend,
        "blog_gap_text": blog_gap_text,
        "datalab_summary": datalab_summary,
        "future_gap_insight": None,
        "total_results": len(rows),
    }
    result["future_gap_insight"] = future_gap_insight(result)
    return result


def metric_to_text(value: Optional[int], suffix: str = "") -> str:
    if value is None:
        return "-"
    return f"{value:,}{suffix}"


def booking_to_text(value: Optional[bool]) -> str:
    if value is True:
        return "Y"
    if value is False:
        return "N"
    return "-"


def build_table(rows: List[dict]) -> str:
    header = "| 순위 | 병원명 | 블로그 | 방문자 리뷰 | 거리 | 이미지 | 예약 |"
    sep = "| --- | --- | ---: | ---: | --- | ---: | --- |"
    body = []
    for row in rows:
        body.append(
            "| "
            f"{row.get('rank', '-')} | "
            f"{row.get('name', '-')} | "
            f"{metric_to_text(row.get('blog_review_count'))} | "
            f"{metric_to_text(row.get('visitor_review_count'))} | "
            f"{row.get('distance_text') or '-'} | "
            f"{metric_to_text(row.get('image_count'))} | "
            f"{booking_to_text(row.get('has_booking'))} |"
        )
    return "\n".join([header, sep] + body)


def action_suggestions(analyses: List[dict], weakness_counter: Counter) -> List[str]:
    actions: List[str] = []
    future_items = [item for item in analyses if item.get("future_gap_insight")]
    for item in future_items[:3]:
        text = item["future_gap_insight"]
        if text and text not in actions:
            actions.append(text)

    weak_first = sorted(
        analyses,
        key=lambda item: (
            0 if item["current_rank"] is None else 1,
            item["current_rank"] if item["current_rank"] is not None else 999,
        ),
    )
    for item in weak_first:
        action = item.get("action")
        if action and action not in actions:
            actions.append(action)
        if item.get("blog_gap_text") and (not action or item["label"] not in action):
            actions.append(f"{item['label']}: {item['blog_gap_text']}")
    if weakness_counter["distance"] and not any("거리" in action for action in actions):
        actions.append("거리 영향이 큰 키워드는 근거리 상권 중심으로 운영하고, 멀어질수록 우선순위를 낮춥니다.")
    if weakness_counter["blog"] and not any("블로그" in action for action in actions):
        actions.append("블로그 후기와 시술 전후 사례를 늘려 상위 경쟁사와의 콘텐츠 격차를 줄입니다.")
    if weakness_counter["image"] and not any("이미지" in action for action in actions):
        actions.append("시술 대표 이미지와 전후 사진을 보강해 플레이스 완성도를 높입니다.")
    if not actions:
        actions.append("현재 상위권 핵심 지표가 유지되고 있어 강한 키워드 방어 운영이 우선입니다.")
    return actions[:4]


def build_markdown_report(payload: dict) -> str:
    lines = [
        f"# {payload['target_name']} 네이버 지도 주간 심층 분석 리포트",
        "",
        f"- 분석일: {payload['run_at']}",
        f"- 분석 대상: {payload['target_name']}",
        "- 기준: 네이버 지도 모바일 오가닉 검색",
        f"- 이번 주 분석 키워드: {', '.join(payload['selected_labels'])}",
        "",
        "## 1. 지역 키워드 순위 요약",
        "",
    ]

    regional = [item for item in payload["analyses"] if item["category"] == "지역 키워드"]
    procedures = [item for item in payload["analyses"] if item["category"] == "시술 키워드"]

    if regional:
        lines.append("| 키워드 | 현재 순위 | 전주 대비 | 인사이트 |")
        lines.append("| --- | ---: | --- | --- |")
        for item in regional:
            insight = item["weaknesses"][0] if item["weaknesses"] else (item["strengths"][0] if item["strengths"] else "특이사항 없음")
            current_rank = item["current_rank"] if item["current_rank"] is not None else "미노출"
            lines.append(f"| {item['label']} | {current_rank} | {item['rank_delta_text']} | {insight} |")
    else:
        lines.append("지역 키워드가 설정되어 있지 않습니다.")

    lines.extend([
        "",
        "## 2. 주력 시술 키워드 순위 요약",
        "",
    ])

    if procedures:
        lines.append("| 키워드 | 현재 순위 | 전주 대비 | 인사이트 |")
        lines.append("| --- | ---: | --- | --- |")
        for item in procedures:
            insight = item["weaknesses"][0] if item["weaknesses"] else (item["strengths"][0] if item["strengths"] else "특이사항 없음")
            current_rank = item["current_rank"] if item["current_rank"] is not None else "미노출"
            lines.append(f"| {item['label']} | {current_rank} | {item['rank_delta_text']} | {insight} |")
    else:
        lines.append("시술 키워드가 선택되지 않았습니다.")

    lines.extend([
        "",
        "## 3. 시술 키워드 인사이트",
        "",
    ])

    if procedures:
        for item in procedures:
            current_rank = item["current_rank"] if item["current_rank"] is not None else "미노출"
            lines.append(f"### {item['label']}")
            lines.append("")
            lines.append(f"- 상태: {item['status_text']}")
            lines.append(f"- 현재 순위: {current_rank}")
            lines.append(f"- 전주 대비: {item['rank_delta_text']}")
            for narrative in item["narratives"]:
                lines.append(f"- 인사이트: {narrative}")
            if item["strengths"]:
                lines.append(f"- 강점: {' / '.join(item['strengths'])}")
            if item["weaknesses"]:
                lines.append(f"- 취약점: {' / '.join(item['weaknesses'])}")
            if not item["strengths"] and not item["weaknesses"]:
                lines.append("- 인사이트: 이번 주 수집 데이터 기준 뚜렷한 우열 포인트가 제한적입니다.")
            lines.append("")
    else:
        lines.append("선택된 시술 키워드가 없습니다.")
        lines.append("")

    lines.extend([
        "## 4. 키워드별 경쟁사 비교",
        "",
    ])

    for item in payload["analyses"]:
        current_rank = item["current_rank"] if item["current_rank"] is not None else "미노출"
        lines.append(f"### {item['label']}")
        lines.append("")
        lines.append(f"- 상태: {item['status_text']}")
        lines.append(f"- 현재 순위: {current_rank}")
        lines.append(f"- 전주 대비: {item['rank_delta_text']}")
        lines.append(f"- 수집 결과 수: {item['total_results']}개")
        for narrative in item["narratives"]:
            lines.append(f"- 인사이트: {narrative}")
        if item["strengths"]:
            lines.append(f"- 강점: {' / '.join(item['strengths'])}")
        if item["weaknesses"]:
            lines.append(f"- 취약점: {' / '.join(item['weaknesses'])}")
        lines.append("")
        lines.append(build_table(item["display_rows"]))
        lines.append("")

    lines.extend([
        "## 5. 취약 포인트 인사이트",
        "",
    ])

    if payload["top_weaknesses"]:
        for text in payload["top_weaknesses"]:
            lines.append(f"- {text}")
    else:
        lines.append("- 이번 주에는 반복적으로 드러난 취약 포인트가 제한적입니다.")

    lines.extend([
        "",
        "## 6. 우선 대응 제안",
        "",
    ])
    for action in payload["actions"]:
        lines.append(f"- {action}")

    return "\n".join(lines).rstrip() + "\n"


def build_slack_summary(payload: dict) -> str:
    lines = [
        "📊 네이버 지도 주간 심층 리포트",
        "────────────────────────",
        f"🏥 대상: {payload['target_name']}",
        f"📅 기준일: {payload['run_at']}",
        f"🔎 분석 키워드: {', '.join(payload['selected_labels'])}",
    ]
    for item in payload["analyses"]:
        current = item["current_rank"] if item["current_rank"] is not None else "미노출"
        lines.append(f"- {item['label']}: {current} / {item['rank_delta_text']}")
    regional = [item for item in payload["analyses"] if item["category"] == "지역 키워드"]
    procedures = [item for item in payload["analyses"] if item["category"] == "시술 키워드"]
    if regional:
        lines.append("📍 지역 키워드 인사이트")
        for item in regional:
            rank_text = item["current_rank"] if item["current_rank"] is not None else "미노출"
            lines.append(f"- {item['label']} {rank_text}위: {item['short_insight']}")
    if procedures:
        lines.append("💉 시술 키워드 인사이트")
        strong = [item for item in procedures if isinstance(item["current_rank"], int) and item["current_rank"] <= 5]
        mid = [item for item in procedures if isinstance(item["current_rank"], int) and 6 <= item["current_rank"] <= 15]
        weak = [item for item in procedures if isinstance(item["current_rank"], int) and item["current_rank"] >= 16]
        missing = [item for item in procedures if item["current_rank"] is None]
        if strong:
            lines.append(f"- 강한 키워드: {summarize_keyword_group(strong)}")
        if mid:
            lines.append(f"- 유지/보완 키워드: {summarize_keyword_group(mid)}")
        if weak:
            lines.append(f"- 하위 노출 키워드: {summarize_keyword_group(weak)}")
        if missing:
            ranked_missing = sorted(
                missing,
                key=lambda item: (
                    0 if (item.get("datalab_summary") or {}).get("direction") == "rising" else 1,
                    -float((item.get("datalab_summary") or {}).get("recent_avg", -1)),
                    item["label"],
                ),
            )
            lines.append(f"- 미노출 키워드: {summarize_keyword_group(ranked_missing, include_rank=False, limit=8)}")
            top_missing = ranked_missing[:3]
            if top_missing:
                details = []
                for item in top_missing:
                    datalab = item.get("datalab_summary") or {}
                    recent_avg = datalab.get("recent_avg")
                    summary = datalab.get("summary")
                    if recent_avg is not None:
                        detail = f"{item['label']} 추세 {summary}"
                        detail += f" (최근 4주 평균 {recent_avg})"
                        details.append(detail)
                if details:
                    lines.append(f"- 우선 보완 후보: {' / '.join(details)}")
        if strong and missing:
            lines.append("- 시사점: 일부 리프팅/기기명 키워드는 상위 노출되지만, 범용 시술 키워드는 아직 미노출 구간이 많습니다.")
        elif missing:
            lines.append("- 시사점: 주력 시술 키워드 다수가 아직 미노출이라 신규 유입용 키워드 자산 보강이 필요합니다.")
        elif strong:
            lines.append("- 시사점: 현재 상위 노출되는 시술 키워드는 방어 운영, 나머지는 확장 운영이 적합합니다.")
    if payload["top_weaknesses"]:
        lines.append("⚠️ 취약 포인트")
        for text in payload["top_weaknesses"][:3]:
            lines.append(f"- {text}")
    lines.append("✅ 권장 액션")
    for action in payload["actions"][:3]:
        lines.append(f"- {action}")
    return "\n".join(lines)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    weekly_hour = int(os.getenv("WEEKLY_HOUR", "11"))
    weekly_minute = int(os.getenv("WEEKLY_MINUTE", "30"))
    force_weekly = os.getenv("FORCE_WEEKLY", "").strip().lower() in ("1", "true", "yes")
    state_path = Path(
        os.getenv(
            "WEEKLY_DEEP_REPORT_STATE_PATH",
            str(project_root / "data" / "weekly_deep_report_last_sent.txt"),
        ).strip()
    )
    output_dir = Path(
        os.getenv(
            "WEEKLY_DEEP_REPORT_OUTPUT_DIR",
            str(project_root / "out"),
        ).strip()
    )
    report_display = int(os.getenv("DEEP_REPORT_DISPLAY", "100"))
    competitor_limit = int(os.getenv("DEEP_REPORT_COMPETITOR_LIMIT", "10"))
    procedure_limit = int(os.getenv("DEEP_REPORT_PROCEDURE_LIMIT", "0"))
    lat = float(os.getenv("RANK_LAT", "37.504444"))
    lon = float(os.getenv("RANK_LON", "127.024444"))
    target_place_id = os.getenv("TARGET_PLACE_ID", "").strip()
    slack_url = (
        os.getenv("SLACK_WEBHOOK_URL_RANK", "").strip()
        or os.getenv("SLACK_WEBHOOK_URL", "").strip()
    )
    config_path = Path(
        os.getenv("RANK_KEYWORD_CONFIG_PATH", str(default_config_path(project_root))).strip()
    )

    config = load_keyword_config(config_path) or {}
    metrics_map = load_keyword_metrics(os.getenv("KEYWORD_CSV_PATH", "data/keyword_metrics.csv").strip())
    datalab_cfg = NaverDataLabConfig(
        client_id=os.getenv("NAVER_DATALAB_CLIENT_ID", "").strip() or os.getenv("NAVER_CLIENT_ID", "").strip(),
        client_secret=os.getenv("NAVER_DATALAB_CLIENT_SECRET", "").strip() or os.getenv("NAVER_CLIENT_SECRET", "").strip(),
    )
    target_name = os.getenv("TARGET_PLACE_NAME", "").strip() or str(config.get("target_name", "세예의원")).strip()

    now = dt.datetime.now()
    if not force_weekly:
        if now.weekday() != 0:
            return
        if (now.hour, now.minute) < (weekly_hour, weekly_minute):
            return
        if state_path.exists() and state_path.read_text(encoding="utf-8").strip() == now.strftime("%Y-%m-%d"):
            return

    keyword_entries = resolve_active_keywords(config) if config else []
    selected_keywords = select_keywords(keyword_entries, procedure_limit=procedure_limit)
    if not selected_keywords:
        raise RuntimeError("활성화된 네이버 플레이스 키워드가 없습니다.")

    today = now.date()
    datalab_trends = {}
    if datalab_cfg.client_id and datalab_cfg.client_secret:
        procedure_keywords = [item["keyword"] for item in selected_keywords if item.get("category") == "시술 키워드"]
        if procedure_keywords:
            end_date = today.strftime("%Y-%m-%d")
            start_date = (today - dt.timedelta(weeks=12)).strftime("%Y-%m-%d")
            datalab_trends = fetch_search_trends(
                cfg=datalab_cfg,
                keywords=procedure_keywords,
                start_date=start_date,
                end_date=end_date,
                time_unit="week",
            )

    rank_logs = load_rank_logs(os.getenv("RANK_LOG_PATH", "data/naver_place_rank_log.jsonl").strip())
    analyses = []
    weakness_counter: Counter = Counter()
    weakness_lines: List[str] = []

    for entry in selected_keywords:
        rows = fetch_places(entry["keyword"], lon, lat, display=report_display)
        previous_rank = latest_previous_rank(rank_logs, entry["keyword"], today)
        metric = resolve_metric(metrics_map, entry["keyword"])
        metric_trend = metric_trend_text(metrics_map, entry["keyword"])
        datalab_summary = resolve_datalab_summary(datalab_trends, entry["keyword"])
        analysis = analyze_keyword(
            entry=entry,
            rows=rows,
            target_name=target_name,
            target_place_id=target_place_id,
            competitor_limit=competitor_limit,
            previous_rank=previous_rank,
            metric=metric,
            metric_trend=metric_trend,
            datalab_summary=datalab_summary,
        )
        analyses.append(analysis)
        weakness_counter.update(analysis["weakness_tags"])
        if analysis["weaknesses"]:
            weakness_lines.append(f"{analysis['label']}: {analysis['weaknesses'][0]}")

    payload = {
        "run_at": now.strftime("%Y-%m-%d %H:%M"),
        "target_name": target_name,
        "target_place_id": target_place_id or None,
        "selected_keywords": [item["keyword"] for item in selected_keywords],
        "selected_labels": [item.get("label", item["keyword"]) for item in selected_keywords],
        "analyses": analyses,
        "top_weaknesses": weakness_lines[:3],
        "actions": action_suggestions(analyses, weakness_counter),
    }

    report_path = output_dir / f"naver_place_deep_report_{today.strftime('%Y-%m-%d')}.md"
    data_path = output_dir / f"naver_place_deep_report_data_{today.strftime('%Y-%m-%d')}.json"

    write_json(data_path, payload)
    write_text(report_path, build_markdown_report(payload))

    summary = build_slack_summary(payload)
    print(summary)
    if slack_url:
        post_webhook(slack_url, summary)

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(today.strftime("%Y-%m-%d"), encoding="utf-8")


if __name__ == "__main__":
    main()
