import os
from pathlib import Path

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException

from naver_place_review_alert import (
    append_review_archive,
    collect_latest_reviews,
    extract_total_review_count,
    hash_review,
    load_sent_log,
    load_settings,
    maybe_switch_to_entry_iframe,
    setup_driver,
    stats_path,
    write_review_stats,
)


def main() -> None:
    settings = load_settings()
    if not settings.naver_place_url:
        raise RuntimeError("NAVER_PLACE_URL is missing. Set it in .env.")

    project_root = Path(__file__).resolve().parents[1]
    archive_stats_path = stats_path(project_root)
    archived_hashes = load_sent_log(settings.review_archive_path)

    driver = setup_driver()
    try:
        print("리뷰 아카이브 동기화 시작")
        try:
            driver.get(settings.naver_place_url)
        except TimeoutException:
            print("페이지 진입 타임아웃: 부분 로딩된 DOM으로 계속 진행합니다.")
        except Exception as exc:
            print(f"페이지 진입 실패: {exc}")
            raise
        print("페이지 진입 완료")
        reviews = collect_latest_reviews(
            driver,
            max_reviews=int(os.getenv("MAX_ARCHIVE_REVIEWS", "50")),
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        total_review_count = extract_total_review_count(soup)
        print(
            f"수집 완료: 리뷰 {len(reviews)}건, "
            f"플레이스 총 리뷰 {total_review_count}건"
        )
    finally:
        driver.quit()

    write_review_stats(
        archive_stats_path,
        total_review_count=total_review_count,
        archived_count=len(archived_hashes),
    )

    added_count = 0
    for review in reviews:
        review_text = review.get("text", "")
        if not review_text:
            continue
        review_hash = hash_review(review_text)
        before_count = len(archived_hashes)
        append_review_archive(
            settings.review_archive_path,
            archived_hashes,
            review_hash,
            review_text,
            review.get("link", ""),
            review.get("date", ""),
        )
        if len(archived_hashes) > before_count:
            added_count += 1

    write_review_stats(
        archive_stats_path,
        total_review_count=total_review_count,
        archived_count=len(archived_hashes),
    )
    print(
        f"리뷰 아카이브 갱신 완료: 추가 {added_count}건, "
        f"누적 저장 {len(archived_hashes)}건, "
        f"플레이스 총 리뷰 {total_review_count}건"
    )


if __name__ == "__main__":
    main()
