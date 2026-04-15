import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional


def default_config_path(project_root: Path) -> Path:
    return project_root / "data" / "naver_place_keywords.json"


def load_keyword_config(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _matches_active_window(item: dict, today: dt.date) -> bool:
    if item.get("enabled", True) is False:
        return False

    active_from = str(item.get("active_from", "")).strip()
    if active_from:
        try:
            if today < dt.date.fromisoformat(active_from):
                return False
        except ValueError:
            return False

    active_to = str(item.get("active_to", "")).strip()
    if active_to:
        try:
            if today > dt.date.fromisoformat(active_to):
                return False
        except ValueError:
            return False

    active_weeks = item.get("active_weeks")
    if active_weeks is not None:
        try:
            weeks = {int(week) for week in active_weeks}
        except (TypeError, ValueError):
            return False
        if today.isocalendar().week not in weeks:
            return False

    return True


def resolve_active_keywords(config: dict, today: Optional[dt.date] = None) -> List[Dict[str, str]]:
    current_date = today or dt.date.today()
    keywords = config.get("keywords", [])
    resolved: List[Dict[str, str]] = []
    for item in keywords:
        if not isinstance(item, dict):
            continue
        keyword = str(item.get("keyword", "")).strip()
        if not keyword:
            continue
        if not _matches_active_window(item, current_date):
            continue
        resolved.append(
            {
                "keyword": keyword,
                "category": str(item.get("category", "기타")).strip() or "기타",
                "label": str(item.get("label", keyword)).strip() or keyword,
                "intent": str(item.get("intent", "")).strip(),
                "note": str(item.get("note", "")).strip(),
            }
        )
    return resolved


def keyword_meta_map(config: Optional[dict], today: Optional[dt.date] = None) -> Dict[str, Dict[str, str]]:
    if not config:
        return {}
    items = resolve_active_keywords(config, today=today)
    return {item["keyword"]: item for item in items}
