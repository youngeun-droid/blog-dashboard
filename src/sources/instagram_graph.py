import time
from typing import Dict, List, Optional

import requests


class InstagramGraphClient:
    def __init__(
        self,
        access_token: str,
        user_id: str,
        version: str = "v19.0",
        base_url: str = "https://graph.facebook.com",
        sleep_seconds: float = 0.0,
    ) -> None:
        self.access_token = access_token
        self.user_id = user_id
        self.version = version
        self.base_url = base_url.rstrip("/")
        self.sleep_seconds = sleep_seconds

    def _get(self, path: str, params: Dict[str, str]) -> Dict:
        url = f"{self.base_url}/{self.version}/{path.lstrip('/')}"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)
        return response.json()

    def _get_next(self, url: str) -> Dict:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)
        return response.json()

    def hashtag_search(self, query: str) -> List[Dict]:
        payload = self._get(
            "ig_hashtag_search",
            {
                "user_id": self.user_id,
                "q": query,
                "access_token": self.access_token,
            },
        )
        return payload.get("data", [])

    def get_hashtag_id(self, query: str) -> Optional[str]:
        results = self.hashtag_search(query)
        if not results:
            return None
        return results[0].get("id")

    def recent_media(self, hashtag_id: str, fields: str, limit: int = 50) -> List[Dict]:
        return self._paginate(
            f"{hashtag_id}/recent_media",
            {
                "user_id": self.user_id,
                "fields": fields,
                "limit": min(limit, 50),
                "access_token": self.access_token,
            },
            limit,
        )

    def top_media(self, hashtag_id: str, fields: str, limit: int = 50) -> List[Dict]:
        return self._paginate(
            f"{hashtag_id}/top_media",
            {
                "user_id": self.user_id,
                "fields": fields,
                "limit": min(limit, 50),
                "access_token": self.access_token,
            },
            limit,
        )

    def _paginate(self, path: str, params: Dict[str, str], max_items: int) -> List[Dict]:
        items: List[Dict] = []
        payload = self._get(path, params)
        items.extend(payload.get("data", []))

        while len(items) < max_items:
            paging = payload.get("paging", {}) if payload else {}
            next_url = paging.get("next")
            if not next_url:
                break
            payload = self._get_next(next_url)
            items.extend(payload.get("data", []))

        return items[:max_items]
