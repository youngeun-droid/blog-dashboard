"""Naver SearchAd official API connector.

This module is intentionally a stub until we load the official API manual/spec.
Please provide the API manual URL or spec file so we can implement the exact
endpoint paths, query params, and signature format.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from ..models import KeywordMetric


@dataclass
class NaverSearchAdConfig:
    access_license: str
    secret_key: str
    customer_id: str
    base_url: str = ""


def fetch_keyword_metrics_weekly(
    cfg: NaverSearchAdConfig,
    seed_keywords: List[str],
) -> List[KeywordMetric]:
    """Fetch keyword metrics via the official SearchAd API.

    TODO: Implement once the official API manual/spec is provided.
    """

    if not (cfg.access_license and cfg.secret_key and cfg.customer_id):
        raise RuntimeError("NAVER SearchAd credentials are missing.")

    raise NotImplementedError(
        "SearchAd API connector not implemented yet. "
        "Provide the official API manual/spec so we can wire the exact endpoints."
    )
