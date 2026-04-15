import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, quote
import re
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

SLACK_WEBHOOK_URL = ""
CSV_FILE = "ranking.csv"
ENCODING = "utf-8-sig"

MOBILE_UA = (
    "Mozilla/5.0 (Linux; Android 10; SM-G973N) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)
DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# API-based parsing has proven unstable; prefer HTML parsing
USE_API_PARSER = False
DEBUG_VERSION = "2026-02-11-1726"


def send_slack(keyword: str, rank: str | int) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        payload = {"text": f"📊 [{keyword}] 순위 체크 완료! (현재 {rank}위)"}
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    except Exception:
        pass


def _extract_blog_id_post_id(raw_url: str) -> tuple[str, str] | None:
    try:
        parsed = urlparse(raw_url)
        host = parsed.netloc.lower()
        path = parsed.path.strip("/")
        if host in {"blog.naver.com", "m.blog.naver.com"}:
            # /{blogId}/{logNo}
            parts = path.split("/")
            if len(parts) >= 2 and parts[0] and parts[1].isdigit():
                return parts[0], parts[1]
            # /PostView.naver?blogId=...&logNo=...
            if parts and parts[0].lower() == "postview.naver":
                qs = parse_qs(parsed.query)
                blog_id = qs.get("blogId", [None])[0]
                log_no = qs.get("logNo", [None])[0]
                if blog_id and log_no:
                    return blog_id, log_no
        # Try query params anywhere
        qs = parse_qs(parsed.query)
        blog_id = qs.get("blogId", [None])[0]
        log_no = qs.get("logNo", [None])[0]
        if blog_id and log_no:
            return blog_id, log_no
        return None
    except Exception:
        return None


def _extract_from_text(text: str) -> tuple[str, str] | None:
    if not text:
        return None
    # Direct path form
    m = re.search(r"(?:https?://)?(?:m\\.)?blog\\.naver\\.com/([^/?#]+)/([0-9]{5,})", text)
    if m:
        return m.group(1), m.group(2)
    # Query form
    m = re.search(r"blogId=([^&]+).*?logNo=([0-9]{5,})", text)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r"logNo=([0-9]{5,}).*?blogId=([^&]+)", text)
    if m:
        return m.group(2), m.group(1)
    return None


def check_rank_selenium(keyword: str, target_url: str, debug: bool = False) -> str | int:
    # Match PC blog tab URL parameters used in manual checks
    url = (
        "https://search.naver.com/search.naver"
        f"?ssc=tab.blog.all&sm=tab_opt&nso=so:r,p:all&query={quote(keyword)}"
    )

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--incognito")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)
        time.sleep(2)

        def collect_links():
            links: list[str] = []
            seen: set[tuple[str, str]] = set()
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='blog.naver.com']")
            for a in anchors:
                href = a.get_attribute("href")
                if not href:
                    continue
                pid = _extract_from_text(href) or _extract_blog_id_post_id(href)
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                links.append(href)
            return links

        last_count = 0
        same_rounds = 0
        links: list[str] = []

        while len(links) < 120 and same_rounds < 3:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            links = collect_links()
            if len(links) == last_count:
                same_rounds += 1
            else:
                same_rounds = 0
                last_count = len(links)

        # Determine rank
        target = _extract_blog_id_post_id(target_url) or _extract_from_text(target_url)
        if debug:
            return "DEBUG:rank=" + (
                str(links.index(next(h for h in links if target and ( _extract_from_text(h) == target))) + 1)
                if target and any(_extract_from_text(h) == target for h in links)
                else "미발견"
            ) + " | " + " | ".join(links[:50])

        if target:
            for idx, href in enumerate(links, start=1):
                if _extract_from_text(href) == target:
                    return idx
        return "100위 밖"
    finally:
        driver.quit()


def check_rank(
    keyword: str,
    target_url: str,
    debug: bool = False,
    snapshot_dir: str | None = None,
    use_selenium: bool = False,
) -> str | int:
    if use_selenium:
        return check_rank_selenium(keyword, target_url, debug=debug)
    target_url = target_url.strip().strip("<>")
    # PC blog tab for consistent ranking vs. search.naver.com "블로그" tab
    base_url = (
        "https://search.naver.com/search.naver"
        f"?ssc=tab.blog.all&sm=tab_opt&nso=so:r,p:all&query={requests.utils.quote(keyword)}"
    )
    # PC blog tab blocks some mobile UAs; use desktop UA
    headers = {"User-Agent": DESKTOP_UA}

    target = _extract_blog_id_post_id(target_url)
    target_log_no = target[1] if target else None

    rank = 0
    seen: set[tuple[str, str]] = set()

    debug_links: list[str] = []
    debug_meta: list[str] = []
    target_hits = 0

    found_rank: int | None = None

    def _fast_rank_from_html(html: str, base_start: int | None) -> str | int | None:
        # Try direct data-url/data-cr-on pairs across the whole HTML
        log_no = None
        if target:
            log_no = target[1]
        if not log_no:
            m = re.search(r"blog\\.naver\\.com/[^/]+/([0-9]{5,})", target_url)
            if m:
                log_no = m.group(1)
        if log_no:
            # Direct regex by logNo (most reliable)
            pat1 = rf"data-url\\s*=\\s*['\\\"]?[^'\\\">\\s]*{re.escape(log_no)}[^'\\\">\\s]*['\\\"]?[^>]*?data-cr-on\\s*=\\s*['\\\"]([^'\\\"]+)"
            pat2 = rf"data-cr-on\\s*=\\s*['\\\"]([^'\\\"]+)['\\\"][^>]*?data-url\\s*=\\s*['\\\"]?[^'\\\">\\s]*{re.escape(log_no)}[^'\\\">\\s]*"
            for pat in (pat1, pat2):
                m = re.search(pat, html)
                if m:
                    s = m.group(1)
                    m2 = re.search(r"r=(\\d+)", s)
                    if m2:
                        r = int(m2.group(1))
                        found = base_start + r - 1 if base_start and r <= 30 else r
                        if debug:
                            links = []
                            seen_local = set()
                            for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                                if href in seen_local:
                                    continue
                                seen_local.add(href)
                                links.append(href)
                                if len(links) >= 50:
                                    break
                            return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH;rank={found} | " + " | ".join(links)
                        return found
            # JSON-ish fallback: find rank near the target logNo in embedded data
            try:
                for mloc in re.finditer(re.escape(log_no), html):
                    start_i = max(0, mloc.start() - 1200)
                    end_i = min(len(html), mloc.end() + 1200)
                    chunk = html[start_i:end_i]
                    m2 = re.search(r"\"r\"\\s*:\\s*(\\d+)", chunk)
                    if not m2:
                        m2 = re.search(r"r=(\\d+)", chunk)
                    if m2:
                        r = int(m2.group(1))
                        found = base_start + r - 1 if base_start and r <= 30 else r
                        if debug:
                            links = []
                            seen_local = set()
                            for href in re.findall(r"https?://blog\.naver\.com/[A-Za-z0-9_\-]+/\d+", html):
                                if href in seen_local:
                                    continue
                                seen_local.add(href)
                                links.append(href)
                                if len(links) >= 50:
                                    break
                            return f"DEBUG:v={DEBUG_VERSION};mode=FASTJSON;rank={found} | " + " | ".join(links)
                        return found
            except Exception:
                pass
            # HTML DOM fallback: locate tag containing logNo and extract r= from its attributes
            try:
                soup_local = BeautifulSoup(html, "html.parser")
                for tag in soup_local.find_all(True):
                    vals = []
                    for _, v in tag.attrs.items():
                        if isinstance(v, str):
                            vals.append(v)
                        elif isinstance(v, list):
                            vals.extend([x for x in v if isinstance(x, str)])
                    if not any(log_no in v for v in vals):
                        continue
                    # Prefer data-cr-on/off in the same tag
                    for key in ("data-cr-on", "data-cr-off"):
                        v = tag.attrs.get(key)
                        if isinstance(v, str):
                            m2 = re.search(r"r=(\\d+)", v)
                            if m2:
                                r = int(m2.group(1))
                                found = base_start + r - 1 if base_start and r <= 30 else r
                                if debug:
                                    links = []
                                    seen_local = set()
                                    for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                                        if href in seen_local:
                                            continue
                                        seen_local.add(href)
                                        links.append(href)
                                        if len(links) >= 50:
                                            break
                                    return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH2;rank={found} | " + " | ".join(links)
                                return found
                    # Fallback: any attribute value with r=
                    for v in vals:
                        m2 = re.search(r"r=(\\d+)", v)
                        if m2:
                            r = int(m2.group(1))
                            found = base_start + r - 1 if base_start and r <= 30 else r
                            if debug:
                                links = []
                                seen_local = set()
                                for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                                    if href in seen_local:
                                        continue
                                    seen_local.add(href)
                                    links.append(href)
                                    if len(links) >= 50:
                                        break
                                return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH2;rank={found} | " + " | ".join(links)
                            return found
            except Exception:
                pass
        # Build a map of blogId/logNo -> item rank from data-url + data-cr-on pairs
        pair_patterns = [
            r"data-url=['\\\"]https?://blog\\.naver\\.com/([^/]+)/([0-9]{5,})['\\\"][^>]*?data-cr-on=['\\\"]([^'\\\"]*?)['\\\"]",
            r"data-cr-on=['\\\"]([^'\\\"]*?)['\\\"][^>]*?data-url=['\\\"]https?://blog\\.naver\\.com/([^/]+)/([0-9]{5,})['\\\"]",
        ]
        rank_map: dict[tuple[str, str], int] = {}
        for pat in pair_patterns:
            for m in re.finditer(pat, html):
                if len(m.groups()) == 3:
                    if "data-url" in pat:
                        blog_id, log_no, s = m.group(1), m.group(2), m.group(3)
                    else:
                        s, blog_id, log_no = m.group(1), m.group(2), m.group(3)
                    m2 = re.search(r"r=(\\d+)", s)
                    if not m2:
                        continue
                    rank_map[(blog_id, log_no)] = int(m2.group(1))

        # Prefer exact blogId/logNo match when possible
        if target and target in rank_map:
            r = rank_map[target]
            found = base_start + r - 1 if base_start and r <= 30 else r
            if debug:
                links = []
                seen_local = set()
                for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                    if href in seen_local:
                        continue
                    seen_local.add(href)
                    links.append(href)
                    if len(links) >= 50:
                        break
                return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH;rank={found} | " + " | ".join(links)
            return found
        # Fallback: direct regex by logNo in the same tag
        if log_no:
            direct_pat = rf"data-url=['\\\"][^'\\\"]*{re.escape(log_no)}[^'\\\"]*['\\\"][^>]*data-cr-on=['\\\"]([^'\\\"]*?)['\\\"]"
            m = re.search(direct_pat, html)
            if m:
                s = m.group(1)
                m2 = re.search(r"r=(\\d+)", s)
                if m2:
                    r = int(m2.group(1))
                    found = base_start + r - 1 if base_start and r <= 30 else r
                    if debug:
                        links = []
                        seen_local = set()
                        for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                            if href in seen_local:
                                continue
                            seen_local.add(href)
                            links.append(href)
                            if len(links) >= 50:
                                break
                        return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH;rank={found} | " + " | ".join(links)
                    return found
        # Fallback: match by logNo only
        if log_no:
            for (blog_id, ln), r in rank_map.items():
                if ln == log_no:
                    found = base_start + r - 1 if base_start and r <= 30 else r
                    if debug:
                        links = []
                        seen_local = set()
                        for href in re.findall(r"https?://blog\\.naver\\.com/[A-Za-z0-9_\\-]+/\\d+", html):
                            if href in seen_local:
                                continue
                            seen_local.add(href)
                            links.append(href)
                            if len(links) >= 50:
                                break
                    return f"DEBUG:v={DEBUG_VERSION};mode=FASTPATH;rank={found} | " + " | ".join(links)
                    return found
        return None

    def process_page(html: str, base_start: int | None = None) -> str | int | None:
        nonlocal rank, found_rank, target_hits
        soup = BeautifulSoup(html, "html.parser")

        def extract_item_rank(item) -> int | None:
            try:
                # Prefer explicit rank from data-cr-on/off attributes
                raw = str(item)
                cr_nums: list[int] = []
                for m in re.findall(r"data-cr-(?:on|off)=\"[^\"]*?r=(\d+)", raw):
                    try:
                        cr_nums.append(int(m))
                    except Exception:
                        pass
                if cr_nums:
                    return min(cr_nums)

                nums: list[int] = []
                for tag in item.find_all(True):
                    for _, val in tag.attrs.items():
                        if not isinstance(val, str):
                            continue
                        for m in re.findall(r"r=(\\d+)", val):
                            try:
                                nums.append(int(m))
                            except Exception:
                                pass
                if nums:
                    return min(nums)
                for t in item.find_all(text=True):
                    s = t.strip()
                    if s.isdigit():
                        n = int(s)
                        if 1 <= n <= 200:
                            return n
                return None
            except Exception:
                return None

        def extract_rank_for_target_item(item, log_no: str) -> int | None:
            try:
                raw = str(item)
                pat = rf"data-url=[\"'][^\"']*{re.escape(log_no)}[^\"']*[\"'][^>]*data-cr-on=[\"']([^\"']+)"
                m = re.search(pat, raw)
                if m:
                    s = m.group(1)
                    m2 = re.search(r"r=(\\d+)", s)
                    if m2:
                        return int(m2.group(1))
                return extract_item_rank(item)
            except Exception:
                return None

        def iter_result_items():
            # Prefer blog tab result containers in order (PC)
            containers = soup.select("ul.lst_type, ul.lst_total, ul.lst_view, div.lst_total, div.lst_view")
            for container in containers:
                items = container.find_all("li", recursive=False)
                if not items:
                    items = container.find_all("li")
                for item in items:
                    yield item

        def process_items(items_list):
            nonlocal rank, found_rank, target_hits
            any_candidates = False
            for item in items_list:
                # If the target logNo appears anywhere in this item, trust item-level rank.
                if target_log_no:
                    raw_item = str(item)
                    if target_log_no in raw_item and found_rank is None:
                        item_r = extract_rank_for_target_item(item, target_log_no)
                        if item_r is not None:
                            current_rank = base_start + item_r - 1 if base_start and item_r <= 30 else item_r
                            found_rank = current_rank
                            if not debug:
                                return current_rank
                # collect all blog post links within the item
                candidates: list[tuple[str, tuple[str, str]]] = []
                for a in item.find_all("a", href=True):
                    href = a["href"]
                    if "adcr.naver.com" in href or "powerlink" in href:
                        continue
                    pid = _extract_blog_id_post_id(href) or _extract_from_text(href)
                    if pid:
                        candidates.append((href, pid))
                for tag in item.find_all(True):
                    for _, val in tag.attrs.items():
                        if not isinstance(val, str):
                            continue
                        if "blog.naver.com" not in val and "blogId=" not in val:
                            continue
                        pid = _extract_blog_id_post_id(val) or _extract_from_text(val)
                        if pid:
                            candidates.append((val, pid))

                if not candidates:
                    continue
                any_candidates = True
                item_rank = extract_item_rank(item)
                if item_rank is None:
                    rank += 1
                    current_rank = rank
                else:
                    if base_start and item_rank <= 30:
                        current_rank = base_start + item_rank - 1
                    else:
                        current_rank = item_rank
                    if item_rank > rank:
                        rank = item_rank
                for href, pid in candidates:
                    if pid in seen:
                        continue
                    seen.add(pid)

                    if target_log_no and pid[1] == target_log_no:
                        target_hits += 1

                    if debug and len(debug_links) < 50:
                        debug_links.append(href)
                    if target and (pid == target or (pid[1] == target[1])) and found_rank is None:
                        if target[1]:
                            item_r = extract_rank_for_target_item(item, target[1])
                            if item_r is not None:
                                current_rank = base_start + item_r - 1 if base_start and item_r <= 30 else item_r
                        found_rank = current_rank
                        if not debug:
                            return current_rank
                    if not target and target_url in href and found_rank is None:
                        found_rank = current_rank
                        if not debug:
                            return current_rank
                    if current_rank >= 100:
                        if not debug and not target:
                            return "100위 밖"
            return any_candidates

        items = list(iter_result_items())
        if items:
            any_candidates = process_items(items)
            if any_candidates is False:
                # Newer Naver blog tab uses Fender cards without <li> wrappers
                items = soup.select("div[data-template-id='ugcItem'], div[data-template-type='searchBasic']")
                if items:
                    process_items(items)
        else:
            # Newer Naver blog tab uses Fender cards without <li> wrappers
            items = soup.select("div[data-template-id='ugcItem'], div[data-template-type='searchBasic']")
            if items:
                process_items(items)
            else:
                # Fallback: DOM order of blog links (no item rank available)
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "adcr.naver.com" in href or "powerlink" in href:
                        continue

                    pid = _extract_blog_id_post_id(href) or _extract_from_text(href)
                    if not pid:
                        continue

                    rank += 1
                    current_rank = rank

                    if pid in seen:
                        continue
                    seen.add(pid)

                    if debug and len(debug_links) < 50:
                        debug_links.append(href)
                    if target and pid == target and found_rank is None:
                        found_rank = current_rank
                        if not debug:
                            return current_rank
                    if not target and target_url in href and found_rank is None:
                        found_rank = current_rank
                        if not debug:
                            return current_rank
                    if current_rank >= 100:
                        if not debug and not target:
                            return "100위 밖"

        # If not found yet, optionally try API-fed items referenced in page scripts
        if USE_API_PARSER:
            api_urls = re.findall(r"https://s\\.search\\.naver\\.com/p/review/[^\"'\\s]+", html)
        else:
            api_urls = []
        for api_url in api_urls:
            try:
                resp_api = requests.get(api_url, headers=headers, timeout=10)
                if resp_api.status_code != 200:
                    continue
                data = resp_api.json()
            except Exception:
                continue

            # Determine base rank from query param (start)
            try:
                start_qs = parse_qs(urlparse(api_url).query)
                base_rank = int(start_qs.get("start", ["1"])[0])
            except Exception:
                base_rank = None

            items_list: list[tuple[tuple[str, str], int | None]] = []

            def walk(obj, current_rank_hint: int | None = None):
                if isinstance(obj, dict):
                    # rank field if exists
                    rank_val = None
                    if "rank" in obj and isinstance(obj["rank"], int):
                        rank_val = obj["rank"]
                    # clickLog r=NN
                    for v in obj.values():
                        if isinstance(v, str):
                            m = re.search(r"r=(\\d+)", v)
                            if m:
                                try:
                                    rank_val = int(m.group(1))
                                except Exception:
                                    pass
                    # Any string fields that contain blogId/logNo
                    for v in obj.values():
                        if isinstance(v, str):
                            pid = _extract_from_text(v)
                            if pid:
                                rank_hint = rank_val if rank_val is not None else current_rank_hint
                                if rank_hint is not None:
                                    items_list.append((pid, rank_hint))
                        walk(v, rank_val if rank_val is not None else current_rank_hint)
                elif isinstance(obj, list):
                    for v in obj:
                        walk(v, current_rank_hint)

            walk(data)

            # De-dup and apply ranking
            seen_api: set[tuple[str, str]] = set()
            for idx, (pid, rank_val) in enumerate(items_list):
                if pid in seen_api:
                    continue
                seen_api.add(pid)
                if pid in seen:
                    continue
                seen.add(pid)
                if rank_val is None:
                    # Skip unranked API entries to avoid inflated ranks
                    continue
                current_rank = rank_val
                if current_rank > rank:
                    rank = current_rank

                if debug and len(debug_links) < 50:
                    debug_links.append(f"https://blog.naver.com/{pid[0]}/{pid[1]}")
                if target and pid == target and found_rank is None:
                    found_rank = current_rank
                    if not debug:
                        return current_rank
                if not target and target_url in f"https://blog.naver.com/{pid[0]}/{pid[1]}" and found_rank is None:
                    found_rank = current_rank
                    if not debug:
                        return current_rank
                if current_rank >= 100:
                    if not debug and not target:
                        return "100위 밖"
        return None

    # Try paginated PC blog results (start=1,31,61,...)
    # Primary: 30 results per page
    for start in range(1, 121, 30):
        url = base_url if start == 1 else f"{base_url}&start={start}"
        url = url + "&display=30"
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 403:
            break
        resp.raise_for_status()
        if debug and target_log_no:
            debug_meta.append(f"p{start}:log={'Y' if target_log_no in resp.text else 'N'}")
        if snapshot_dir:
            try:
                os.makedirs(snapshot_dir, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                snap_path = os.path.join(snapshot_dir, f"naver_blog_{quote(keyword)}_{start}_{ts}.html")
                with open(snap_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
            except Exception:
                pass
        fast = _fast_rank_from_html(resp.text, base_start=start)
        if fast is not None:
            return fast
        result = process_page(resp.text, base_start=start)
        if result is not None:
            return result
    # Fallback: 10 results per page
    for start in range(1, 101, 10):
        url = base_url if start == 1 else f"{base_url}&start={start}"
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 403:
            break
        resp.raise_for_status()
        if debug and target_log_no:
            debug_meta.append(f"p{start}:log={'Y' if target_log_no in resp.text else 'N'}")
        fast = _fast_rank_from_html(resp.text, base_start=start)
        if fast is not None:
            return fast
        result = process_page(resp.text, base_start=start)
        if result is not None:
            return result

    if debug:
        rank_text = str(found_rank) if found_rank is not None else "미발견"
        meta = ""
        if target:
            meta = f";target={target[0]}/{target[1]};hits={target_hits}"
        if debug_meta:
            meta += ";pages=" + ",".join(debug_meta[:6])
        return f"DEBUG:v={DEBUG_VERSION};mode=HTML;rank={rank_text}{meta} | " + " | ".join(debug_links)

    return "100위 밖"


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["발행일시", "키워드", "포스팅URL", "2시간순위", "1일순위", "7일순위"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]


def track_once(csv_path: str = CSV_FILE, notify: bool = True) -> List[Dict[str, Any]]:
    df = pd.read_csv(csv_path, encoding=ENCODING)
    df = _ensure_columns(df)

    now = datetime.now()
    updated = False
    updates: List[Dict[str, Any]] = []

    for idx, row in df.iterrows():
        try:
            published = datetime.strptime(str(row["발행일시"]), "%Y-%m-%d %H:%M")
            elapsed = now - published

            need_check = False
            if elapsed >= timedelta(hours=2) and pd.isna(row["2시간순위"]):
                need_check = True
            if elapsed >= timedelta(hours=24) and pd.isna(row["1일순위"]):
                need_check = True
            if elapsed >= timedelta(days=7) and pd.isna(row["7일순위"]):
                need_check = True

            if not need_check:
                continue

            keyword = str(row["키워드"])
            post_url = str(row["포스팅URL"])

            rank = check_rank(keyword, post_url)

            row_updated = False
            if elapsed >= timedelta(hours=2) and pd.isna(row["2시간순위"]):
                df.at[idx, "2시간순위"] = rank
                row_updated = True
            if elapsed >= timedelta(hours=24) and pd.isna(row["1일순위"]):
                df.at[idx, "1일순위"] = rank
                row_updated = True
            if elapsed >= timedelta(days=7) and pd.isna(row["7일순위"]):
                df.at[idx, "7일순위"] = rank
                row_updated = True

            if row_updated:
                updated = True
                updates.append({"키워드": keyword, "포스팅URL": post_url, "순위": rank})
                if notify:
                    send_slack(keyword, rank)

            time.sleep(1)
        except Exception:
            continue

    if updated:
        df.to_csv(csv_path, index=False, encoding=ENCODING)

    return updates


def append_row(csv_path: str, keyword: str, post_url: str, published_at: datetime | None = None) -> None:
    if published_at is None:
        published_at = datetime.now()

    try:
        df = pd.read_csv(csv_path, encoding=ENCODING)
        df = _ensure_columns(df)
    except Exception:
        df = pd.DataFrame(columns=["발행일시", "키워드", "포스팅URL", "2시간순위", "1일순위", "7일순위"])

    new_row = {
        "발행일시": published_at.strftime("%Y-%m-%d %H:%M"),
        "키워드": keyword,
        "포스팅URL": post_url,
        "2시간순위": pd.NA,
        "1일순위": pd.NA,
        "7일순위": pd.NA,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(csv_path, index=False, encoding=ENCODING)

def force_check_and_update(
    csv_path: str,
    keyword: str,
    post_url: str,
    published_at: datetime | None = None,
    debug: bool = False,
    snapshot_dir: str | None = None,
    use_selenium: bool = False,
    rank_override: str | int | None = None,
) -> str | int:
    if published_at is None:
        published_at = datetime.now()

    try:
        df = pd.read_csv(csv_path, encoding=ENCODING)
        df = _ensure_columns(df)
    except Exception:
        df = pd.DataFrame(columns=["발행일시", "키워드", "포스팅URL", "2시간순위", "1일순위", "7일순위"])

    # Append new row
    new_row = {
        "발행일시": published_at.strftime("%Y-%m-%d %H:%M"),
        "키워드": keyword,
        "포스팅URL": post_url,
        "2시간순위": pd.NA,
        "1일순위": pd.NA,
        "7일순위": pd.NA,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    # Force immediate rank check (or use provided rank) and store to the first empty rank column
    if rank_override is None:
        rank = check_rank(keyword, post_url, debug=debug, snapshot_dir=snapshot_dir, use_selenium=use_selenium)
    else:
        rank = rank_override
    idx = df.index[-1]
    for col in ["2시간순위", "1일순위", "7일순위"]:
        if pd.isna(df.at[idx, col]):
            df.at[idx, col] = rank
            break

    df.to_csv(csv_path, index=False, encoding=ENCODING)
    return rank


def check_rank_stable(
    keyword: str,
    post_url: str,
    attempts: int = 3,
    delay_sec: float = 1.0,
    snapshot_dir: str | None = None,
    use_selenium: bool = False,
) -> str | int:
    # Run multiple checks and return median for stability.
    vals: list[int] = []
    real_vals: list[int] = []
    for i in range(attempts):
        try:
            r = check_rank(keyword, post_url, debug=False, snapshot_dir=snapshot_dir, use_selenium=use_selenium)
            if isinstance(r, int):
                vals.append(r)
                real_vals.append(r)
            elif isinstance(r, str) and r.strip() == "100위 밖":
                vals.append(101)
        except Exception:
            pass
        if i < attempts - 1:
            time.sleep(delay_sec)
    if not vals:
        return "100위 밖"
    # If any real ranks are found, prefer median of those to avoid flapping to 100+
    if real_vals:
        real_vals.sort()
        med = real_vals[len(real_vals) // 2]
    else:
        vals.sort()
        med = vals[len(vals) // 2]
    return "100위 밖" if med >= 101 else med

def main() -> None:
    track_once(CSV_FILE, notify=True)


if __name__ == "__main__":
    main()
