import csv
from typing import List

from ..models import KeywordMetric


def load_csv(path: str) -> List[KeywordMetric]:
    items: List[KeywordMetric] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keyword = (row.get("keyword") or "").strip()
            if not keyword:
                continue
            items.append(
                KeywordMetric(
                    keyword=keyword,
                    search_volume=int(float(row.get("search_volume") or 0)),
                    saturation=float(row.get("saturation") or 0),
                    source=(row.get("source") or "manual").strip(),
                    updated_at=(row.get("updated_at") or "").strip(),
                )
            )
    return items
