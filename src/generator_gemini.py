from __future__ import annotations

import os
from typing import Optional

from .models import Draft


def render_prompt(template_path: str, title: str, topic: str, primary_keyword: str) -> str:
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()
    return (
        template.replace("{{title}}", title)
        .replace("{{topic}}", topic)
        .replace("{{primary_keyword}}", primary_keyword)
    )


def generate_with_gemini(
    api_key: str,
    model: str,
    prompt: str,
    title: str,
    topic: str,
    primary_keyword: str,
) -> Draft:
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing. Set it in .env before generating drafts.")

    # NOTE: We keep the actual API call out of this repo to avoid stale endpoints.
    # Plug in the official Gemini SDK or your preferred HTTP call here.
    # Return Draft(body=generated_text, checklist=generated_checklist)
    raise NotImplementedError(
        "Gemini call not wired yet. Plug in the official SDK here."
    )
