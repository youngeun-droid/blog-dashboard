import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


@dataclass
class Settings:
    # Gemini
    gemini_api_key: str
    gemini_model: str

    # Naver SearchAd
    naver_access_license: str
    naver_secret_key: str
    naver_customer_id: str
    naver_base_url: str

    # Data
    keyword_csv_path: str

    # Output
    output_dir: str

    # Selection rules
    saturation_max: float
    daily_total: int
    daily_low_vol_count: int
    low_vol_max: int
    daily_high_vol_count: int
    high_vol_min: int

    # Draft generation
    draft_char_target: int
    primary_keyword_count: int

    # Instagram
    instagram_access_token: str
    instagram_user_id: str
    instagram_graph_version: str
    instagram_keyword_xlsx_path: str
    instagram_keyword_sheet: str
    instagram_header_scan_rows: int
    instagram_output_dir: str
    instagram_hashtag_daily_limit: int
    instagram_max_media_per_tag: int
    instagram_top_tags: int
    instagram_top_media: int
    instagram_comment_weight: float
    instagram_fields: str
    instagram_state_path: str
    instagram_sleep_seconds: float

    # Slack
    slack_webhook_url: str

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip(),
            naver_access_license=os.getenv("NAVER_SEARCHAD_ACCESS_LICENSE", "").strip(),
            naver_secret_key=os.getenv("NAVER_SEARCHAD_SECRET_KEY", "").strip(),
            naver_customer_id=os.getenv("NAVER_SEARCHAD_CUSTOMER_ID", "").strip(),
            naver_base_url=os.getenv("NAVER_SEARCHAD_BASE_URL", "").strip(),
            keyword_csv_path=os.getenv("KEYWORD_CSV_PATH", "data/keyword_metrics.csv").strip(),
            output_dir=os.getenv("OUTPUT_DIR", "out").strip(),
            saturation_max=_env_float("SATURATION_MAX", 0.40),
            daily_total=_env_int("DAILY_TOTAL", 5),
            daily_low_vol_count=_env_int("DAILY_LOW_VOL_COUNT", 2),
            low_vol_max=_env_int("LOW_VOL_MAX", 999),
            daily_high_vol_count=_env_int("DAILY_HIGH_VOL_COUNT", 3),
            high_vol_min=_env_int("HIGH_VOL_MIN", 1000),
            draft_char_target=_env_int("DRAFT_CHAR_TARGET", 1300),
            primary_keyword_count=_env_int("PRIMARY_KEYWORD_COUNT", 6),
            instagram_access_token=os.getenv("INSTAGRAM_ACCESS_TOKEN", "").strip(),
            instagram_user_id=os.getenv("INSTAGRAM_USER_ID", "").strip(),
            instagram_graph_version=os.getenv("INSTAGRAM_GRAPH_VERSION", "v19.0").strip(),
            instagram_keyword_xlsx_path=os.getenv(
                "INSTAGRAM_KEYWORD_XLSX_PATH", "[세예] 네이버 키워드 발굴.xlsx"
            ).strip(),
            instagram_keyword_sheet=os.getenv("INSTAGRAM_KEYWORD_SHEET", "").strip(),
            instagram_header_scan_rows=_env_int("INSTAGRAM_HEADER_SCAN_ROWS", 30),
            instagram_output_dir=os.getenv("INSTAGRAM_OUTPUT_DIR", "out/instagram").strip(),
            instagram_hashtag_daily_limit=_env_int("INSTAGRAM_HASHTAG_DAILY_LIMIT", 30),
            instagram_max_media_per_tag=_env_int("INSTAGRAM_MAX_MEDIA_PER_TAG", 50),
            instagram_top_tags=_env_int("INSTAGRAM_TOP_TAGS", 5),
            instagram_top_media=_env_int("INSTAGRAM_TOP_MEDIA", 5),
            instagram_comment_weight=_env_float("INSTAGRAM_COMMENT_WEIGHT", 2.0),
            instagram_fields=os.getenv(
                "INSTAGRAM_FIELDS",
                "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count,thumbnail_url",
            ).strip(),
            instagram_state_path=os.getenv("INSTAGRAM_STATE_PATH", "data/instagram_state.json").strip(),
            instagram_sleep_seconds=_env_float("INSTAGRAM_SLEEP_SECONDS", 0.0),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", "").strip(),
        )
