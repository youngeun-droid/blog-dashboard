from dataclasses import dataclass
from typing import Optional


@dataclass
class KeywordMetric:
    keyword: str
    search_volume: int
    saturation: float
    source: str
    updated_at: str


@dataclass
class KeywordCandidate:
    metric: KeywordMetric
    score: float
    bucket: str  # "low" or "high"


@dataclass
class Draft:
    keyword: str
    title: str
    topic: str
    body: str
    checklist: Optional[str] = None
