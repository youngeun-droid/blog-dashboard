import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from .config import Settings
from .slack import post_webhook
from .sources.instagram_graph import InstagramGraphClient
from .sources.keyword_xlsx import load_keywords_xlsx


def _today() -> str:
    return date.today().isoformat()


def _load_state(path: str) -> Dict:
    state_path = Path(path)
    if not state_path.exists():
        return {"last_run": None, "recent_hashtags": []}
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_state(path: str, state: Dict) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _prune_recent(state: Dict, days: int = 7) -> Dict:
    cutoff = date.today() - timedelta(days=days)
    keep: List[Dict] = []
    for item in state.get("recent_hashtags", []):
        try:
            item_date = datetime.fromisoformat(item.get("date", "")).date()
        except ValueError:
            continue
        if item_date >= cutoff:
            keep.append(item)
    state["recent_hashtags"] = keep
    return state


def _select_candidates(
    keywords: List[str], state: Dict, daily_limit: int
) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    recent = {item.get("tag") for item in state.get("recent_hashtags", [])}

    fresh = [k for k in keywords if k not in recent]
    picked = fresh[:daily_limit]

    if len(picked) < daily_limit:
        warnings.append("Not enough fresh hashtags under 7-day limit. Reusing older tags.")
        remaining = [k for k in keywords if k not in picked]
        picked.extend(remaining[: max(0, daily_limit - len(picked))])

    if not picked:
        warnings.append("No hashtags available after filtering.")

    return picked, warnings


def _score_media(media: Dict, comment_weight: float) -> float:
    like_count = int(media.get("like_count") or 0)
    comments_count = int(media.get("comments_count") or 0)
    return like_count + comments_count * comment_weight


def _summarize_media(media: Dict, score: float) -> Dict:
    return {
        "id": media.get("id"),
        "permalink": media.get("permalink"),
        "media_type": media.get("media_type"),
        "timestamp": media.get("timestamp"),
        "like_count": int(media.get("like_count") or 0),
        "comments_count": int(media.get("comments_count") or 0),
        "score": round(score, 2),
        "caption": (media.get("caption") or "").strip(),
    }


def _build_slack_message(date_str: str, trends: List[Dict]) -> str:
    lines: List[str] = [f"[{date_str}] 인스타그램 시술 급상승 키워드 TOP{len(trends)}"]
    for idx, trend in enumerate(trends, start=1):
        tag = trend["hashtag"]
        score = trend["trend_score"]
        lines.append(f"{idx}) #{tag} (점수 {score})")
        for jdx, media in enumerate(trend["top_media"], start=1):
            like_count = media["like_count"]
            comments_count = media["comments_count"]
            permalink = media["permalink"]
            lines.append(
                f"- {jdx}) 좋아요 {like_count} / 댓글 {comments_count} {permalink}"
            )
    return "\n".join(lines)


def run_instagram_daily() -> None:
    settings = Settings.load()

    if not settings.instagram_access_token or not settings.instagram_user_id:
        raise RuntimeError("INSTAGRAM_ACCESS_TOKEN and INSTAGRAM_USER_ID are required.")

    keywords, sheet_name = load_keywords_xlsx(
        settings.instagram_keyword_xlsx_path,
        sheet_name=settings.instagram_keyword_sheet,
        header_scan_rows=settings.instagram_header_scan_rows,
    )
    if not keywords:
        raise RuntimeError("No keywords found in the Excel file.")

    state = _prune_recent(_load_state(settings.instagram_state_path))
    candidates, warnings = _select_candidates(
        keywords, state, settings.instagram_hashtag_daily_limit
    )

    client = InstagramGraphClient(
        access_token=settings.instagram_access_token,
        user_id=settings.instagram_user_id,
        version=settings.instagram_graph_version,
        sleep_seconds=settings.instagram_sleep_seconds,
    )

    results: List[Dict] = []

    for tag in candidates:
        hashtag_id = client.get_hashtag_id(tag)
        if not hashtag_id:
            warnings.append(f"Hashtag not found: {tag}")
            continue

        media = client.recent_media(
            hashtag_id=hashtag_id,
            fields=settings.instagram_fields,
            limit=settings.instagram_max_media_per_tag,
        )

        scored = [
            (m, _score_media(m, settings.instagram_comment_weight)) for m in media
        ]
        scored.sort(key=lambda item: item[1], reverse=True)

        top_media = [
            _summarize_media(m, score)
            for m, score in scored[: settings.instagram_top_media]
        ]

        if top_media:
            avg_score = sum(m["score"] for m in top_media) / len(top_media)
        else:
            avg_score = 0.0

        results.append(
            {
                "hashtag": tag,
                "hashtag_id": hashtag_id,
                "trend_score": round(avg_score, 2),
                "media_count": len(media),
                "top_media": top_media,
            }
        )

    results.sort(key=lambda item: item["trend_score"], reverse=True)
    top_trends = results[: settings.instagram_top_tags]

    today = _today()
    output_dir = Path(settings.instagram_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "date": today,
        "sheet": sheet_name,
        "candidate_count": len(candidates),
        "top_tags": settings.instagram_top_tags,
        "top_media": settings.instagram_top_media,
        "warnings": warnings,
        "trends": top_trends,
    }

    json_path = output_dir / f"{today}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    if settings.slack_webhook_url:
        message = _build_slack_message(today, top_trends)
        post_webhook(settings.slack_webhook_url, message)

    state["last_run"] = today
    for tag in candidates:
        state["recent_hashtags"].append({"tag": tag, "date": today})
    _save_state(settings.instagram_state_path, state)


if __name__ == "__main__":
    run_instagram_daily()
