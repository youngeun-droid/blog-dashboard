import datetime as dt
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from .naver_place_keyword_config import (
        default_config_path,
        load_keyword_config,
        resolve_active_keywords,
    )
    from .naver_place_review_alert import post_webhook, setup_driver
except ImportError:
    from naver_place_keyword_config import default_config_path, load_keyword_config, resolve_active_keywords
    from naver_place_review_alert import post_webhook, setup_driver


def build_search_url(query: str, x: float, y: float, start: int, display: int) -> str:
    q = quote_plus(query)
    return (
        "https://m.place.naver.com/place/list"
        f"?query={q}"
        f"&x={x}&y={y}"
        f"&start={start}&display={display}"
        "&deviceType=mobile&sortingOrder=precision&level=top"
    )


def extract_next_data(page_source: str) -> Optional[dict]:
    soup = BeautifulSoup(page_source, "html.parser")
    node = soup.select_one("script#__NEXT_DATA__")
    if not node or not node.string:
        return None
    try:
        return json.loads(node.string)
    except json.JSONDecodeError:
        return None


NAME_KEYS = ("name", "title", "placeName", "businessName")
RANK_KEYS = ("rank", "rankOrder", "order", "listIndex", "seq")
AD_KEYS = ("isAd", "isAdvertisement", "isSponsored", "ad", "adYn", "adType")


def find_place_lists(obj) -> List[List[dict]]:
    found: List[List[dict]] = []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            if any(any(key in item for key in NAME_KEYS) for item in obj):
                found.append(obj)
        for item in obj:
            found.extend(find_place_lists(item))
    elif isinstance(obj, dict):
        for value in obj.values():
            found.extend(find_place_lists(value))
    return found


def score_place_list(items: List[dict]) -> int:
    score = 0
    for item in items[:10]:
        if any(k in item for k in ("id", "placeId", "businessId")):
            score += 1
        if any(k in item for k in NAME_KEYS):
            score += 1
        if any(k in item for k in ("category", "bizCategory", "businessCategory")):
            score += 1
        if any(k in item for k in RANK_KEYS):
            score += 2
    return score


def extract_place_items_from_next(data: dict) -> List[dict]:
    lists = find_place_lists(data)
    if not lists:
        return []
    lists.sort(key=score_place_list, reverse=True)
    organic = lists[0]
    ad_list: Optional[List[dict]] = None
    for candidate in lists:
        if candidate is organic:
            continue
        if any(
            any(item.get(k) for k in AD_KEYS if k in item) for item in candidate
        ):
            ad_list = candidate
            break
    return (ad_list + organic) if ad_list else organic


def split_ads(items: List[dict]) -> Tuple[List[dict], List[dict]]:
    ads: List[dict] = []
    organic: List[dict] = []
    for item in items:
        is_ad = any(item.get(k) for k in AD_KEYS if k in item)
        if is_ad:
            ads.append(item)
        else:
            organic.append(item)
    return ads, organic


def order_by_rank(items: List[dict]) -> List[dict]:
    def rank_value(obj: dict) -> int:
        for key in RANK_KEYS:
            if key in obj:
                try:
                    return int(obj[key])
                except Exception:
                    continue
        return 10**9

    if any(k in item for item in items for k in RANK_KEYS):
        return sorted(items, key=rank_value)
    return items


def extract_place_names_from_next(data: dict) -> List[str]:
    items = extract_place_items_from_next(data)
    if not items:
        return []

    ads, organic = split_ads(items)
    ordered_ads = order_by_rank(ads)
    ordered_org = order_by_rank(organic)
    merged = ordered_ads + ordered_org if ads else ordered_org

    names: List[str] = []
    for item in merged:
        for key in NAME_KEYS:
            name = item.get(key)
            if name:
                names.append(str(name).strip())
                break
    return [n for n in names if n]


def extract_place_names_from_html(page_source: str) -> List[str]:
    soup = BeautifulSoup(page_source, "html.parser")
    names: List[str] = []
    seen = set()
    for anchor in soup.select("a[href*='/place/']"):
        text = anchor.get_text(" ", strip=True)
        if not text or len(text) < 2:
            continue
        if text in seen:
            continue
        seen.add(text)
        names.append(text)
    return names


def maybe_switch_search_iframe(driver, wait) -> None:
    try:
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))
    except Exception:
        return


def dump_debug(query: str, names: List[str]) -> None:
    os.makedirs("out", exist_ok=True)
    path = os.path.join("out", f"rank_debug_{query}.txt")
    with open(path, "w", encoding="utf-8") as f:
        for idx, name in enumerate(names, start=1):
            f.write(f"{idx}. {name}\n")


def append_rank_log(path: str, entry: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False))
        f.write("\n")


def find_rank(names: List[str], target: str) -> Optional[int]:
    target_norm = target.replace(" ", "")
    for idx, name in enumerate(names, start=1):
        if target_norm in name.replace(" ", ""):
            return idx
    return None


def build_message(
    query: str,
    target: str,
    rank: Optional[int],
    total: int,
    lat: float,
    lon: float,
) -> str:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    if rank:
        result = f"{target} {rank}위 (총 {total}개 중)"
    else:
        result = f"{target} 검색 결과에서 찾지 못함 (총 {total}개 중)"
    return (
        "📍 네이버 플레이스 순위 체크\n"
        "────────────────────────\n"
        f"🕒 기준 시간: {ts}\n"
        f"🔎 키워드: {query}\n"
        f"📱 기준: 모바일 / 광고 포함 / 비로그인\n"
        f"🏥 결과: {result}"
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    target_name = os.getenv("TARGET_PLACE_NAME", "세예의원").strip()
    lat = float(os.getenv("RANK_LAT", "37.504444"))
    lon = float(os.getenv("RANK_LON", "127.024444"))
    display = int(os.getenv("RANK_DISPLAY", "50"))
    slack_url = (
        os.getenv("SLACK_WEBHOOK_URL_RANK", "").strip()
        or os.getenv("SLACK_WEBHOOK_URL", "").strip()
    )
    disable_slack = os.getenv("RANK_DISABLE_SLACK", "").strip().lower() in ("1", "true", "yes")
    log_path = os.getenv("RANK_LOG_PATH", "data/naver_place_rank_log.jsonl").strip()
    map_search_urls = os.getenv("NAVER_MAP_SEARCH_URLS", "").strip()
    debug_dump = os.getenv("RANK_DEBUG_DUMP", "").strip().lower() in ("1", "true", "yes")
    use_keyword_config = os.getenv("RANK_USE_KEYWORD_CONFIG", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    config_path = Path(
        os.getenv("RANK_KEYWORD_CONFIG_PATH", str(default_config_path(project_root))).strip()
    )

    keyword_entries = []
    if use_keyword_config:
        keyword_config = load_keyword_config(config_path)
        keyword_entries = resolve_active_keywords(keyword_config) if keyword_config else []

    if not keyword_entries:
        keywords = os.getenv("RANK_KEYWORDS", "신논현피부과,강남역피부과")
        keyword_entries = [
            {
                "keyword": q.strip(),
                "category": "지역 키워드",
                "label": q.strip(),
                "intent": "",
                "note": "",
            }
            for q in keywords.split(",")
            if q.strip()
        ]

    mobile_ua = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    )

    driver = setup_driver(user_agent=mobile_ua)
    driver.set_window_size(390, 844)
    wait = WebDriverWait(driver, 15)

    try:
        url_map = {}
        if map_search_urls:
            for pair in map_search_urls.split("|"):
                if not pair.strip():
                    continue
                if "::" not in pair:
                    continue
                key, value = pair.split("::", 1)
                url_map[key.strip()] = value.strip()

        for entry in keyword_entries:
            query = entry["keyword"]
            url = url_map.get(query)
            if not url:
                url = build_search_url(query, lon, lat, start=1, display=display)
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            maybe_switch_search_iframe(driver, wait)

            data = extract_next_data(driver.page_source)
            names = extract_place_names_from_next(data) if data else []
            if not names:
                names = extract_place_names_from_html(driver.page_source)

            if debug_dump:
                dump_debug(query, names)

            rank = find_rank(names, target_name)
            append_rank_log(
                log_path,
                {
                    "date": dt.datetime.now().strftime("%Y-%m-%d"),
                    "time": dt.datetime.now().strftime("%H:%M"),
                    "keyword": query,
                    "category": entry.get("category", "기타"),
                    "label": entry.get("label", query),
                    "intent": entry.get("intent", ""),
                    "note": entry.get("note", ""),
                    "target": target_name,
                    "rank": rank,
                    "total": len(names),
                },
            )
            message = build_message(
                query=query,
                target=target_name,
                rank=rank,
                total=len(names),
                lat=lat,
                lon=lon,
            )
            print(message)
            if slack_url and not disable_slack:
                try:
                    post_webhook(slack_url, message)
                except Exception as exc:
                    print(f"[Slack 전송 실패] {exc}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
