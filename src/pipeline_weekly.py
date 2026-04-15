import csv
from datetime import date
from pathlib import Path

from .config import Settings
from .sources.naver_searchad import NaverSearchAdConfig, fetch_keyword_metrics_weekly


SEED_KEYWORDS_PATH = "data/seed_keywords.txt"


def _load_seed_keywords(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def run_weekly() -> None:
    settings = Settings.load()

    seeds = _load_seed_keywords(SEED_KEYWORDS_PATH)
    if not seeds:
        raise RuntimeError(
            "No seed keywords found. Add keywords to data/seed_keywords.txt"
        )

    cfg = NaverSearchAdConfig(
        access_license=settings.naver_access_license,
        secret_key=settings.naver_secret_key,
        customer_id=settings.naver_customer_id,
        base_url=settings.naver_base_url,
    )

    metrics = fetch_keyword_metrics_weekly(cfg, seeds)

    out_path = Path(settings.keyword_csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "search_volume", "saturation", "source", "updated_at"])
        for m in metrics:
            writer.writerow([m.keyword, m.search_volume, m.saturation, m.source, m.updated_at])

    print(f"Wrote {len(metrics)} rows to {out_path}")


if __name__ == "__main__":
    run_weekly()
