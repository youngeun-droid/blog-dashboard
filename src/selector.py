from typing import List, Tuple

from .models import KeywordCandidate, KeywordMetric


def _score(metric: KeywordMetric) -> float:
    # Prefer higher volume and lower saturation
    return metric.search_volume * (1.0 - metric.saturation)


def select_keywords(
    metrics: List[KeywordMetric],
    saturation_max: float,
    low_vol_max: int,
    high_vol_min: int,
    low_count: int,
    high_count: int,
) -> Tuple[List[KeywordCandidate], List[str]]:
    warnings: List[str] = []

    eligible = [m for m in metrics if m.saturation <= saturation_max]
    if not eligible:
        warnings.append("No keywords under saturation threshold. Using all metrics as fallback.")
        eligible = metrics[:]

    low = [m for m in eligible if m.search_volume <= low_vol_max]
    high = [m for m in eligible if m.search_volume >= high_vol_min]

    low_sorted = sorted(low, key=_score, reverse=True)
    high_sorted = sorted(high, key=_score, reverse=True)

    selected: List[KeywordCandidate] = []

    if len(low_sorted) < low_count:
        warnings.append("Insufficient low-volume keywords. Filling with best remaining eligible.")
    if len(high_sorted) < high_count:
        warnings.append("Insufficient high-volume keywords. Filling with best remaining eligible.")

    for m in low_sorted[:low_count]:
        selected.append(KeywordCandidate(metric=m, score=_score(m), bucket="low"))

    for m in high_sorted[:high_count]:
        selected.append(KeywordCandidate(metric=m, score=_score(m), bucket="high"))

    # Fill if counts short
    if len(selected) < (low_count + high_count):
        already = {c.metric.keyword for c in selected}
        remaining = [m for m in eligible if m.keyword not in already]
        remaining_sorted = sorted(remaining, key=_score, reverse=True)
        for m in remaining_sorted:
            if len(selected) >= (low_count + high_count):
                break
            selected.append(KeywordCandidate(metric=m, score=_score(m), bucket="fill"))

    return selected, warnings
