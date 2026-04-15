#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import datetime as dt
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote, urlparse, parse_qs, unquote

# ============================
# 수정 포인트: 키워드 리스트
# ============================
KEYWORDS = [
    "울쎄라", "써마지", "올타이트", "온다리프팅", "소프웨이브",
    "엘란쎄", "실리프팅", "티타늄리프팅", "힐로웨이브", "리투오"
]

# Slack Webhook URL (환경변수 권장)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("환경변수 SLACK_WEBHOOK_URL을 설정해 주세요.")

# 선택: 24시간 형식 "HH:MM" (예: 09:30)
# 설정하면 매일 해당 시각에 자동 실행됨
RUN_DAILY_AT = os.getenv("RUN_DAILY_AT", "").strip()
DEBUG_HTML = os.getenv("DEBUG_HTML", "").strip()

# 사용 가능한 뷰어 사이트 우선순위 (쉼표 구분)
# 예: "pictame,picuki"
IG_VIEWER_PROVIDERS = os.getenv("IG_VIEWER_PROVIDERS", "pictame,picuki")

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SESSION = requests.Session()
SESSION.headers.update(BASE_HEADERS)


def sanitize_text(text, max_len=20):
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) > max_len:
        return text[:max_len] + "…"
    return text


def normalize_url(base_url, raw_url):
    if not raw_url:
        return ""
    if raw_url.startswith("data:") or raw_url.startswith("blob:"):
        return ""
    # 이미 알려진 로고/플레이스홀더는 제외
    if "pictame.com/_next/image" in raw_url and "logo.png" in raw_url:
        return ""
    # Pictame 이미지 프록시 URL은 너무 길 수 있으므로 원본 URL로 복원
    if "pictame.com/api/image" in raw_url or "pictame.com/_next/image" in raw_url:
        try:
            parsed = urlparse(raw_url)
            qs = parse_qs(parsed.query)
            if "url" in qs and qs["url"]:
                raw_url = unquote(qs["url"][0])
                # 쿼리가 한번 더 인코딩된 경우 한 번 더 디코딩
                if "%" in raw_url:
                    raw_url = unquote(raw_url)
        except Exception:
            pass
    if raw_url.startswith("//"):
        return "https:" + raw_url
    if raw_url.startswith("http://") or raw_url.startswith("https://"):
        return raw_url
    return urljoin(base_url, raw_url)


def pick_image_url(img, base_url):
    if not img:
        return ""
    # srcset 우선
    srcset = img.get("srcset") or img.get("data-srcset") or ""
    if srcset:
        # 가장 첫 번째 URL 선택
        first = srcset.split(",")[0].strip().split(" ")[0].strip()
        url = normalize_url(base_url, first)
        if url:
            return url
    raw = img.get("src") or img.get("data-src") or img.get("data-lazy") or ""
    return normalize_url(base_url, raw)


def decode_pictame_image_url(src, base_url):
    if not src:
        return ""
    full = normalize_url(base_url, src)
    return full


def extract_pictame_posts(soup, base_url, limit=6):
    # 1) JSON 데이터에서 reels 추출 (가장 안정적)
    reels_posts = extract_pictame_reels_from_json(str(soup), limit=limit, base_url=base_url)
    if reels_posts:
        return reels_posts

    # 2) "Trending Reels" 섹션을 찾는다 (HTML fallback)
    header = None
    for h2 in soup.find_all("h2"):
        if "Trending Reels" in h2.get_text(strip=True):
            header = h2
            break
    if not header:
        return []

    # 섹션 컨테이너에서 카드 그리드 찾기
    section = header.find_parent()
    grid = None
    if section:
        grid = section.find_next("div", class_=re.compile(r"grid"))
    if not grid:
        return []

    posts = []
    cards = grid.find_all(attrs={"data-slot": "card"})
    for card in cards:
        img = card.find("img", src=True)
        img_url = decode_pictame_image_url(img.get("src") if img else "", base_url)
        if not img_url:
            # lazy 이미지: 카드 HTML에서 /api/image?url= 추출
            card_html = str(card)
            m = re.search(r'/api/image\?url=[^"\\s>]+', card_html)
            if m:
                img_url = decode_pictame_image_url(m.group(0), base_url)
        if not img_url:
            continue

        # 캡션 (alt 또는 하단 p)
        caption = ""
        if img and img.get("alt"):
            alt = img.get("alt")
            if " - " in alt:
                caption = alt.split(" - ", 1)[1]
            else:
                caption = alt
        p = card.find("p")
        if p:
            caption = p.get_text(strip=True) or caption
        caption = sanitize_text(caption, max_len=20)

        # 링크: 카드 내부의 프로필 링크 사용
        link = ""
        a = card.find("a", href=True)
        if a:
            link = normalize_url(base_url, a.get("href"))
        if not link:
            m = re.search(r'/en/instagram/[A-Za-z0-9._]+', str(card))
            if m:
                link = normalize_url(base_url, m.group(0))

        # 조회수 (좋아요 대신 표시)
        views = ""
        badge = card.find("span", class_=re.compile(r"badge", re.I))
        if badge:
            txt = badge.get_text(strip=True)
            if re.search(r"\d", txt):
                views = txt

        posts.append({
            "img_url": img_url,
            "link": link,
            "caption": caption,
            "likes": views,
        })
        if len(posts) >= limit:
            break

    return posts


def extract_pictame_reels_from_json(html, limit, base_url):
    # __next_f에 포함된 JSON에서 reels 배열 추출 (이스케이프된 문자열 형태)
    reels = None
    m = re.search(r'\\\\\"reels\\\\\":(\\[.*?\\])\\\\\"?,\\\\\"locale\\\\\"', html, re.S)
    if m:
        arr_escaped = m.group(1)
        try:
            arr_unescaped = arr_escaped.encode("utf-8").decode("unicode_escape")
            reels = json.loads(arr_unescaped)
        except Exception:
            reels = None

    # fallback: 비이스케이프 형태 시도
    if not reels:
        m2 = re.search(r'\"reels\":(\\[.*?\\])\\s*,\\s*\"locale\"', html, re.S)
        if m2:
            try:
                reels = json.loads(m2.group(1))
            except Exception:
                reels = None

    if not reels:
        return []

    posts = []
    for r in reels:
        display = r.get("displayUrl", "") or r.get("display_url", "")
        code = r.get("code", "") or r.get("shortcode", "")
        caption = r.get("caption", "") or ""
        play_count = r.get("playCount", "")
        img_url = normalize_url(base_url, display)
        if not img_url:
            continue
        link = ""
        if code:
            link = f"https://www.instagram.com/reel/{code}/"
        if not link and r.get("id"):
            link = f"https://www.instagram.com/p/{r.get('id')}/"
        posts.append({
            "img_url": img_url,
            "link": link,
            "caption": sanitize_text(caption, max_len=20),
            "likes": str(play_count) if play_count else "",
        })
        if len(posts) >= limit:
            break
    return posts


def parse_like_count(card):
    like_text = ""
    for candidate in card.find_all(["span", "div"], class_=re.compile(r"like|likes|stat|meta", re.I)):
        t = candidate.get_text(strip=True)
        if re.search(r"\d", t):
            like_text = t
            break
    return like_text


def find_top_posts_section(soup):
    headings = soup.find_all(["h2", "h3", "div", "span"])
    for h in headings:
        t = h.get_text(strip=True).lower()
        if "top posts" in t or "top post" in t:
            return h
    return None


def extract_posts_from_section(section):
    posts = []
    container = section.find_parent() if section else None
    if container:
        cards = container.find_all("div", class_=re.compile(r"post|item|box|card", re.I))
        posts.extend(cards)
    return posts


def extract_posts_fallback(soup):
    return soup.find_all("div", class_=re.compile(r"post|item|box|card", re.I))


def extract_post_data(card, base_url):
    # 게시물 링크로 보이는 a 태그 우선
    link = ""
    img = None
    for a in card.find_all("a", href=True):
        href = a.get("href") or ""
        if re.search(r"/p/|/reel/|/tv/|/instagram/", href):
            link = normalize_url(base_url, href)
            img = a.find("img")
            break
    if not link:
        a = card.find("a", href=True)
        link = normalize_url(base_url, a.get("href") if a else "")
        img = card.find("img")

    img_url = pick_image_url(img, base_url)

    caption = ""
    caption_node = card.find("div", class_=re.compile(r"caption|text|desc|content", re.I))
    if caption_node:
        caption = sanitize_text(caption_node.get_text(strip=True), max_len=20)

    likes = parse_like_count(card)

    return {
        "img_url": img_url,
        "link": link,
        "caption": caption,
        "likes": likes,
    }


def build_candidate_urls(provider, encoded_keyword):
    if provider == "pictame":
        return [
            f"https://pictame.com/en/discover/hashtag/{encoded_keyword}",
            f"https://pictame.com/discover/hashtag/{encoded_keyword}",
            f"https://pictame.com/tag/{encoded_keyword}",
            f"https://pictame.com/en/tag/{encoded_keyword}",
            f"https://pictame.com/hashtag/{encoded_keyword}",
            f"https://pictame.com/en/hashtag/{encoded_keyword}",
        ]
    if provider == "picuki":
        return [
            f"https://www.picuki.com/tag/{encoded_keyword}",
            f"https://picuki.com/tag/{encoded_keyword}",
        ]
    return []


def fetch_html(keyword, provider):
    # 한글 키워드는 URL 인코딩 필요
    encoded = quote(keyword, safe="")
    candidate_urls = build_candidate_urls(provider, encoded)

    last_error = None
    last_status = None

    for url in candidate_urls:
        try:
            headers = dict(BASE_HEADERS)
            if provider == "picuki":
                headers["Referer"] = "https://www.picuki.com/"
            elif provider == "pictame":
                headers["Referer"] = "https://pictame.com/"
            resp = SESSION.get(url, headers=headers, timeout=20, allow_redirects=True)
            last_status = resp.status_code
            if resp.status_code == 403:
                last_error = f"HTTPError 403 for url: {url}"
                continue
            # Pictame: No Reels Found 페이지면 다음 후보로
            if provider == "pictame" and "No Reels Found" in resp.text:
                last_error = f"No reels found at url: {url}"
                continue
            resp.raise_for_status()
            return url, resp
        except Exception as e:
            last_error = repr(e)
            continue

    return None, (last_error, last_status)


def extract_post_candidates(soup):
    # 일반적인 패턴으로 게시물 링크/이미지 추출
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        if re.search(r"/p/|/reel/|/tv/|/instagram/", href):
            img = a.find("img")
            candidates.append((a, img))
    return candidates


def fetch_top_posts(keyword, limit=6):
    providers = [p.strip() for p in IG_VIEWER_PROVIDERS.split(",") if p.strip()]
    last_debug = None

    for provider in providers:
        url, resp = fetch_html(keyword, provider)
        if resp is None or isinstance(resp, tuple):
            if DEBUG_HTML:
                debug_dir = os.path.join("out", "picuki")
                os.makedirs(debug_dir, exist_ok=True)
                safe_kw = re.sub(r"[^0-9A-Za-z_가-힣]+", "_", keyword)
                debug_path = os.path.join(debug_dir, f"{provider}_{safe_kw}_debug.json")
                error, status = resp if isinstance(resp, tuple) else (None, None)
                debug_payload = {
                    "provider": provider,
                    "keyword": keyword,
                    "url": url,
                    "error": error,
                    "status_code": status,
                }
                with open(debug_path, "w", encoding="utf-8") as f:
                    json.dump(debug_payload, f, ensure_ascii=False, indent=2)
                last_debug = debug_path
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        posts = []
        if provider == "pictame":
            posts = extract_pictame_reels_from_json(resp.text, limit=limit, base_url=url)
            if not posts:
                posts = extract_pictame_posts(soup, url, limit=limit)
        else:
            section = find_top_posts_section(soup)
            cards = extract_posts_from_section(section) if section else []
            if not cards:
                cards = extract_posts_fallback(soup)

            for card in cards:
                data = extract_post_data(card, url)
                if data["img_url"] and data["link"]:
                    posts.append(data)
                if len(posts) >= limit:
                    break

            if not posts:
                # fallback: 링크/이미지 기반 탐색
                seen = set()
                for a, img in extract_post_candidates(soup):
                    link = normalize_url(url, a.get("href"))
                    img_url = pick_image_url(img, url)
                    if not link or not img_url:
                        continue
                    if link in seen:
                        continue
                    seen.add(link)
                    posts.append({
                        "img_url": img_url,
                        "link": link,
                        "caption": "",
                        "likes": "",
                    })
                    if len(posts) >= limit:
                        break

        if DEBUG_HTML:
            debug_dir = os.path.join("out", "picuki")
            os.makedirs(debug_dir, exist_ok=True)
            safe_kw = re.sub(r"[^0-9A-Za-z_가-힣]+", "_", keyword)
            html_path = os.path.join(debug_dir, f"{provider}_{safe_kw}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(resp.text)

            debug_payload = {
                "provider": provider,
                "keyword": keyword,
                "url": url,
                "status_code": resp.status_code,
                "cards_found": len(cards),
                "posts_extracted": len(posts),
            }
            debug_path = os.path.join(debug_dir, f"{provider}_{safe_kw}_debug.json")
            with open(debug_path, "w", encoding="utf-8") as f:
                json.dump(debug_payload, f, ensure_ascii=False, indent=2)
            last_debug = debug_path

        if posts:
            return posts

    return []


def build_pair_blocks(posts):
    blocks = []
    pair = []
    for p in posts:
        if not p.get("img_url"):
            continue
        pair.append(p)
        if len(pair) == 2:
            blocks.extend(pair_to_blocks(pair))
            pair = []
    if pair:
        blocks.extend(pair_to_blocks(pair))
    return blocks


def validate_blocks(blocks):
    cleaned = []
    for b in blocks:
        btype = b.get("type")
        if btype == "image":
            url = b.get("image_url", "")
            if not url or not url.startswith("http"):
                continue
            # Slack URL 길이 제한 회피 (너무 긴 URL은 제외)
            if len(url) > 1800:
                continue
            if not b.get("alt_text"):
                b["alt_text"] = "image"
        if btype == "header":
            text = b.get("text", {})
            if not isinstance(text, dict) or not text.get("text"):
                continue
        if btype == "section":
            text = b.get("text", {})
            if not isinstance(text, dict) or not text.get("text"):
                continue
        if btype == "context":
            elements = b.get("elements", [])
            if not elements:
                continue
            # 빈/홈 링크 제거
            filtered = []
            for el in elements:
                t = el.get("text", "")
                if "pictame.com/en|게시물 링크" in t:
                    continue
                filtered.append(el)
            if not filtered:
                continue
            b["elements"] = filtered
            # context 요소는 최대 10개 제한
            if len(elements) > 10:
                b["elements"] = elements[:10]
        cleaned.append(b)
    return cleaned


def pair_to_blocks(pair):
    blocks = []
    for p in pair:
        blocks.append({
            "type": "image",
            "image_url": p["img_url"],
            "alt_text": "post image"
        })
    # 두 개 링크를 한 줄로 보이게 context로 묶음
    context_elems = []
    for p in pair:
        meta_parts = []
        if p["caption"]:
            meta_parts.append(f"`{p['caption']}`")
        if p["likes"]:
            meta_parts.append(f"좋아요: {p['likes']}")
        meta_line = " · ".join(meta_parts) if meta_parts else ""
        link_line = f"<{p['link']}|게시물 링크>"
        if meta_line:
            link_line = f"{link_line} ({meta_line})"
        context_elems.append({"type": "mrkdwn", "text": link_line})

    blocks.append({"type": "context", "elements": context_elems})
    blocks.append({"type": "divider"})
    return blocks


def build_combined_blocks(results_by_keyword):
    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "📢 인기 게시물 Top 6 리포트"}
    })
    blocks.append({"type": "divider"})

    for kw, posts in results_by_keyword.items():
        posts = [p for p in posts if p.get("img_url") and p.get("link")]
        if not posts:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"⚠️ *[{kw}]* 인기 게시물을 찾지 못했습니다."}
            })
            blocks.append({"type": "divider"})
            continue

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*[{kw}]* 인기 게시물 Top {len(posts)}"}
        })
        blocks.extend(build_pair_blocks(posts))

    return blocks


def send_to_slack(blocks):
    payload = {"blocks": blocks}
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if r.status_code >= 400:
        os.makedirs("out", exist_ok=True)
        with open(os.path.join("out", "slack_error.txt"), "w", encoding="utf-8") as f:
            f.write(f"status={r.status_code}\n")
            f.write(r.text or "")
        with open(os.path.join("out", "slack_payload.json"), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        r.raise_for_status()


def send_blocks_in_batches(blocks, max_blocks=45):
    # Slack Block Kit 제한(50개) 대비 여유를 둠
    blocks = validate_blocks(blocks)
    if len(blocks) <= max_blocks:
        send_to_slack(blocks)
        return

    first_chunk = True
    start = 0
    while start < len(blocks):
        chunk = blocks[start:start + max_blocks]
        if not first_chunk:
            # 이어지는 메시지 구분 헤더 추가
            prefix = [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*[계속]*"}
            }, {"type": "divider"}]
            chunk = prefix + chunk
            # prefix가 붙었으면 길이 제한을 넘기지 않게 자르기
            chunk = chunk[:max_blocks]

        send_to_slack(chunk)
        time.sleep(0.6)
        first_chunk = False
        start += max_blocks


def chunk_list(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]


def run_once(group_size=2):
    # 키워드를 그룹 단위로 묶어서 메시지 전송
    for group in chunk_list(KEYWORDS, group_size):
        results_by_keyword = {}
        for kw in group:
            try:
                posts = fetch_top_posts(kw, limit=6)
                results_by_keyword[kw] = posts
                time.sleep(0.8)
            except Exception:
                results_by_keyword[kw] = []
                time.sleep(0.5)

        blocks = build_combined_blocks(results_by_keyword)
        send_blocks_in_batches(blocks)


def run_daily_loop(run_at):
    # run_at: "HH:MM"
    hour, minute = map(int, run_at.split(":"))
    while True:
        now = dt.datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target = target + dt.timedelta(days=1)
        sleep_secs = (target - now).total_seconds()
        time.sleep(sleep_secs)
        try:
            run_once()
        except Exception:
            pass


def main():
    if RUN_DAILY_AT:
        run_daily_loop(RUN_DAILY_AT)
    else:
        run_once()


if __name__ == "__main__":
    main()
