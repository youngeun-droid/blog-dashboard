from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests


API_URL = "https://openapi.naver.com/v1/datalab/search"


@dataclass
class NaverDataLabConfig:
    client_id: str
    client_secret: str


def _batched(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_search_trends(
    cfg: NaverDataLabConfig,
    keywords: List[str],
    start_date: str,
    end_date: str,
    time_unit: str = "week",
    device: str = "",
) -> Dict[str, List[dict]]:
    if not (cfg.client_id and cfg.client_secret):
        return {}

    headers = {
        "X-Naver-Client-Id": cfg.client_id,
        "X-Naver-Client-Secret": cfg.client_secret,
        "Content-Type": "application/json",
    }

    results: Dict[str, List[dict]] = {}
    for batch in _batched(keywords, 5):
        payload = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "keywordGroups": [{"groupName": keyword, "keywords": [keyword]} for keyword in batch],
        }
        if device:
            payload["device"] = device

        response = requests.post(API_URL, headers=headers, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        for item in data.get("results", []):
            title = str(item.get("title", "")).strip()
            if not title:
                continue
            rows = []
            for point in item.get("data", []):
                rows.append(
                    {
                        "period": str(point.get("period", "")).strip(),
                        "ratio": float(point.get("ratio", 0.0)),
                    }
                )
            results[title] = rows
    return results


def summarize_trend(points: List[dict]) -> Optional[dict]:
    if len(points) < 4:
        return None

    ratios = [float(point.get("ratio", 0.0)) for point in points]
    if not ratios:
        return None

    recent = ratios[-4:]
    previous = ratios[-8:-4] if len(ratios) >= 8 else ratios[:-4]
    if not previous:
        previous = ratios[:-1]
    if not previous:
        return None

    recent_avg = sum(recent) / len(recent)
    prev_avg = sum(previous) / len(previous)
    delta = recent_avg - prev_avg
    latest = ratios[-1]

    if prev_avg <= 0:
        direction = "rising" if recent_avg > 0 else "flat"
    elif recent_avg >= prev_avg * 1.15 and delta >= 5:
        direction = "rising"
    elif recent_avg <= prev_avg * 0.85 and delta <= -5:
        direction = "falling"
    else:
        direction = "flat"

    if direction == "rising":
        summary = "최근 8주 대비 최근 4주 상승"
    elif direction == "falling":
        summary = "최근 8주 대비 최근 4주 하락"
    else:
        summary = "최근 8주 기준 보합"

    return {
        "direction": direction,
        "summary": summary,
        "recent_avg": round(recent_avg, 2),
        "previous_avg": round(prev_avg, 2),
        "latest_ratio": round(latest, 2),
        "delta": round(delta, 2),
    }
