import json
import os
from datetime import date
from pathlib import Path
from typing import List

from .config import Settings
from .models import Draft
from .selector import select_keywords
from .sources.manual_csv import load_csv
from .generator_gemini import render_prompt, generate_with_gemini


PROMPT_TEMPLATE = "prompts/gemini_prompt.md"


def _slugify(text: str) -> str:
    return (
        text.strip()
        .replace(" ", "-")
        .replace("/", "-")
        .replace("\\", "-")
    )


def run_daily() -> None:
    settings = Settings.load()

    metrics = load_csv(settings.keyword_csv_path)
    selected, warnings = select_keywords(
        metrics=metrics,
        saturation_max=settings.saturation_max,
        low_vol_max=settings.low_vol_max,
        high_vol_min=settings.high_vol_min,
        low_count=settings.daily_low_vol_count,
        high_count=settings.daily_high_vol_count,
    )

    today = date.today().isoformat()
    output_dir = Path(settings.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    drafts_dir = output_dir / today
    drafts_dir.mkdir(parents=True, exist_ok=True)

    output_payload = {
        "date": today,
        "rules": {
            "saturation_max": settings.saturation_max,
            "low_vol_max": settings.low_vol_max,
            "high_vol_min": settings.high_vol_min,
            "low_count": settings.daily_low_vol_count,
            "high_count": settings.daily_high_vol_count,
        },
        "warnings": warnings,
        "selected": [
            {
                "keyword": c.metric.keyword,
                "search_volume": c.metric.search_volume,
                "saturation": c.metric.saturation,
                "source": c.metric.source,
                "updated_at": c.metric.updated_at,
                "score": round(c.score, 2),
                "bucket": c.bucket,
            }
            for c in selected
        ],
        "drafts": [],
    }

    for c in selected:
        title = f"{c.metric.keyword}에 대해 꼭 알아야 할 내용"
        topic = c.metric.keyword
        prompt = render_prompt(PROMPT_TEMPLATE, title, topic, c.metric.keyword)

        try:
            draft = generate_with_gemini(
                api_key=settings.gemini_api_key,
                model=settings.gemini_model,
                prompt=prompt,
                title=title,
                topic=topic,
                primary_keyword=c.metric.keyword,
            )
            body = draft.body
            checklist = draft.checklist
        except Exception as e:
            body = f"[DRAFT_NOT_GENERATED] {e}"
            checklist = None

        md_path = drafts_dir / f"{_slugify(c.metric.keyword)}.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            f.write(body)
            if checklist:
                f.write("\n\n")
                f.write(checklist)

        output_payload["drafts"].append(
            {
                "keyword": c.metric.keyword,
                "title": title,
                "topic": topic,
                "draft_path": str(md_path),
            }
        )

    json_path = output_dir / f"{today}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    run_daily()
