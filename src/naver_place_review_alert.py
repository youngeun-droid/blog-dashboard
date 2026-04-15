import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import google.generativeai as genai


@dataclass
class Settings:
    naver_place_url: str
    gemini_api_key: str
    gemini_model: str
    review_slack_webhook_url: str
    review_log_path: str
    review_archive_path: str


REVIEW_TEXT_SELECTORS = [
    "a[data-pui-click-code='rvshowless']",
    "a[data-pui-click-code='rvshowmore']",
]


def load_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")
    return Settings(
        naver_place_url=os.getenv("NAVER_PLACE_URL", "").strip(),
        gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-1.5-pro").strip(),
        review_slack_webhook_url=(
            os.getenv("SLACK_WEBHOOK_URL_REVIEW", "").strip()
            or os.getenv("SLACK_WEBHOOK_URL", "").strip()
        ),
        review_log_path=os.getenv(
            "REVIEW_LOG_PATH", str(project_root / "data" / "naver_place_review_log.jsonl")
        ).strip(),
        review_archive_path=os.getenv(
            "REVIEW_ARCHIVE_PATH",
            str(project_root / "data" / "naver_place_review_archive.jsonl"),
        ).strip(),
    )


def stats_path(project_root: Path) -> str:
    return str(project_root / "data" / "naver_place_review_stats.json")


def post_webhook(webhook_url: str, text: str | dict) -> None:
    payload = text if isinstance(text, dict) else {"text": text}
    response = requests.post(webhook_url, json=payload, timeout=30)
    response.raise_for_status()


def setup_driver(user_agent: Optional[str] = None) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.page_load_strategy = "eager"
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR")
    default_ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
    options.add_argument(f"--user-agent={user_agent or default_ua}")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(int(os.getenv("PAGE_LOAD_TIMEOUT", "20")))
    return driver


def maybe_switch_to_entry_iframe(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    try:
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
    except TimeoutException:
        return


def click_by_text(driver: webdriver.Chrome, wait: WebDriverWait, texts: List[str]) -> bool:
    for label in texts:
        xpath = f"//*[self::a or self::button][contains(normalize-space(), '{label}')]"
        try:
            element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", element)
            return True
        except TimeoutException:
            continue
    return False


def click_more_reviews(driver: webdriver.Chrome) -> bool:
    xpaths = [
        "//*[self::a or self::button][contains(normalize-space(), '펼쳐서 더보기')]",
        "//*[self::a or self::button][contains(normalize-space(), '더보기')]",
    ]
    for xpath in xpaths:
        elements = driver.find_elements(By.XPATH, xpath)
        for element in elements:
            if not element.is_displayed():
                continue
            try:
                driver.execute_script("arguments[0].click();", element)
                return True
            except Exception:
                continue
    return False


def extract_review_texts(soup: BeautifulSoup) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    seen: Set[str] = set()
    date_pattern = re.compile(r"(20\d{2})년\s*(\d{1,2})월\s*(\d{1,2})일")

    review_items = soup.select("#_review_list > li")
    for item in review_items:
        text = ""
        for selector in REVIEW_TEXT_SELECTORS:
            node = item.select_one(selector)
            if node:
                text = node.get_text(" ", strip=True)
                break

        if not text:
            continue
        if len(text) < 2:
            continue
        if text in ("사진", "이미지", "포토"):
            continue
        if text in seen:
            continue

        link = ""
        link_node = item.select_one("a[data-pui-click-code='reply']")
        if link_node and link_node.get("href"):
            link = link_node["href"]

        review_date = ""
        for blind in item.select("span.pui__blind"):
            match = date_pattern.search(blind.get_text(" ", strip=True))
            if match:
                year, month, day = match.groups()
                review_date = f"{year}-{int(month):02d}-{int(day):02d}"
                break

        seen.add(text)
        collected.append({"text": text, "link": link, "date": review_date})

    return collected


def extract_total_review_count(soup: BeautifulSoup) -> int:
    selectors = [
        ".place_section_count",
        "h2 .place_section_count",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        digits = re.sub(r"[^0-9]", "", node.get_text(" ", strip=True))
        if digits:
            return int(digits)
    return 0


def collect_latest_reviews(driver: webdriver.Chrome, max_reviews: int = 10) -> List[Dict[str, str]]:
    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    maybe_switch_to_entry_iframe(driver, wait)

    click_by_text(driver, wait, ["리뷰"])
    time.sleep(1.0)
    click_by_text(driver, wait, ["최신순", "최신순으로", "최신"])

    reviews: List[Dict[str, str]] = []
    stagnant_rounds = 0
    previous_count = 0

    for _ in range(20):
        soup = BeautifulSoup(driver.page_source, "html.parser")
        reviews = extract_review_texts(soup)
        if len(reviews) >= max_reviews:
            return reviews[:max_reviews]

        if click_more_reviews(driver):
            time.sleep(1.5)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            reviews = extract_review_texts(soup)
            if len(reviews) >= max_reviews:
                return reviews[:max_reviews]

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

        if len(reviews) <= previous_count:
            stagnant_rounds += 1
            if stagnant_rounds >= 3:
                break
        else:
            stagnant_rounds = 0
        previous_count = len(reviews)

    return reviews[:max_reviews]


def normalize_gemini_json(raw_text: str) -> Optional[dict]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`").strip()
        if text.startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def analyze_reviews_batch(
    model: genai.GenerativeModel, reviews: List[Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    prompt = (
        "다음 리뷰 텍스트를 각각 분석해서 감정을 '긍정', '중립', '부정' 중 하나로 분류해줘. "
        "만약 매장에 대한 불만, 서비스 지적, 맛에 대한 혹평 등이 포함되어 있다면 반드시 '부정'으로 분류해. "
        "결과는 반드시 JSON 배열 형태로, 각 항목에 {'id': 'r1', 'sentiment': '부정', 'reason': '한 줄 요약'} "
        "형태로 줘. 다른 설명은 하지 말고 JSON만 출력해.\n\n"
    )
    prompt += "\n".join([f"{item['id']}: {item['text']}" for item in reviews])

    response = model.generate_content(prompt)
    parsed = normalize_gemini_json(response.text)
    if not isinstance(parsed, list):
        return {}

    results: Dict[str, Dict[str, str]] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        review_id = str(item.get("id", "")).strip()
        if not review_id:
            continue
        results[review_id] = {
            "sentiment": str(item.get("sentiment", "중립")).strip(),
            "reason": str(item.get("reason", "")).strip(),
        }
    return results


def load_sent_log(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    hashes: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line)
                if "hash" in data:
                    hashes.add(data["hash"])
            except json.JSONDecodeError:
                continue
    return hashes


def append_sent_log(path: str, review_hash: str, review_text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {"hash": review_hash, "text": review_text, "ts": timestamp},
                ensure_ascii=False,
            )
        )
        f.write("\n")


def append_review_archive(
    path: str,
    archived_hashes: Set[str],
    review_hash: str,
    review_text: str,
    review_link: str,
    review_date: str,
) -> None:
    if review_hash in archived_hashes:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "hash": review_hash,
                    "text": review_text,
                    "link": review_link,
                    "date": review_date,
                    "ts": timestamp,
                },
                ensure_ascii=False,
            )
        )
        f.write("\n")
    archived_hashes.add(review_hash)


def write_review_stats(path: str, total_review_count: int, archived_count: int) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "total_review_count": total_review_count,
        "archived_count": archived_count,
        "last_sync_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def hash_review(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_slack_message(review_text: str, reason: str, review_link: str) -> str:
    return (
        "[네이버 플레이스 부정 리뷰 감지]\n"
        f"- 리뷰 원문: {review_text}\n"
        f"- 불만 사유: {reason}\n"
        f"- 리뷰 링크: {review_link or '링크 없음'}"
    )


def main() -> None:
    settings = load_settings()
    if not settings.naver_place_url:
        raise RuntimeError("NAVER_PLACE_URL is missing. Set it in .env.")
    if not settings.gemini_api_key:
        raise RuntimeError("GEMINI_API_KEY is missing. Set it in .env.")
    if not settings.review_slack_webhook_url:
        raise RuntimeError(
            "SLACK_WEBHOOK_URL_REVIEW is missing. Set it in .env "
            "(or keep SLACK_WEBHOOK_URL as a fallback)."
        )

    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel(settings.gemini_model)

    driver = setup_driver()
    try:
        try:
            driver.get(settings.naver_place_url)
        except TimeoutException:
            print("페이지 로드 타임아웃: 부분 로딩된 DOM으로 계속 진행합니다.")
        max_reviews = int(os.getenv("MAX_REVIEWS", "10"))
        reviews = collect_latest_reviews(driver, max_reviews=max_reviews)
    finally:
        driver.quit()

    if not reviews:
        print("수집된 리뷰가 없습니다.")
        return

    sent_hashes = load_sent_log(settings.review_log_path)
    archived_hashes = load_sent_log(settings.review_archive_path)

    pending: List[Dict[str, str]] = []
    id_to_text: Dict[str, str] = {}

    for review in reviews:
        review_text = review.get("text", "")
        review_link = review.get("link", "")
        review_date = review.get("date", "")
        if not review_text:
            continue
        review_hash = hash_review(review_text)
        append_review_archive(
            settings.review_archive_path,
            archived_hashes,
            review_hash,
            review_text,
            review_link,
            review_date,
        )
        if review_hash in sent_hashes:
            continue
        review_id = f"r{len(pending)+1}"
        pending.append(
            {
                "id": review_id,
                "text": review_text,
                "hash": review_hash,
                "link": review_link,
                "date": review_date,
            }
        )
        id_to_text[review_id] = review_text

    if os.getenv("ARCHIVE_ONLY", "").strip().lower() in ("1", "true", "yes"):
        print("리뷰 아카이브만 갱신했습니다.")
        return

    if not pending:
        print("신규 리뷰가 없습니다.")
        return

    analysis = analyze_reviews_batch(model, pending)
    if not analysis:
        print("Gemini 응답 파싱에 실패했습니다.")
        return

    for item in pending:
        review_id = item["id"]
        review_text = item["text"]
        review_hash = item["hash"]
        review_link = item.get("link", "")
        result = analysis.get(review_id, {})
        sentiment = result.get("sentiment", "중립")
        reason = result.get("reason", "")
        print(f"[분석] id={review_id} sentiment={sentiment} reason={reason}")
        if sentiment == "부정":
            message = build_slack_message(
                review_text=review_text,
                reason=reason or "불만 사유 확인 필요",
                review_link=review_link or settings.naver_place_url,
            )
            post_webhook(settings.review_slack_webhook_url, message)
            append_sent_log(settings.review_log_path, review_hash, review_text)
            sent_hashes.add(review_hash)


if __name__ == "__main__":
    main()
