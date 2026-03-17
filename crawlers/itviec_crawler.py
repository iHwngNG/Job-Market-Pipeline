"""
ITviec Job Crawler — Playwright + CDP (Connect vào Chrome thật)
---------------------------------------------------------------
Strategy:
  - Connect trực tiếp vào Chrome thật đang chạy qua CDP port 9222
  - TLS fingerprint, JS engine, cookies — 100% real browser
  - Cloudflare không phân biệt được với người dùng bình thường

Setup (làm 1 lần trước khi chạy):
  1. Đóng toàn bộ Chrome đang mở
  2. Mở Chrome với CDP port:

     Windows (PowerShell):
       & "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\\chrome-cdp-profile"

     macOS:
       /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-cdp-profile

     Linux:
       google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-cdp-profile

  3. Vào https://itviec.com trong Chrome vừa mở
  4. Vượt Cloudflare challenge bằng tay nếu bị chặn
  5. Chạy crawler — nó sẽ tự connect vào Chrome đang mở

Usage:
    python itviec_crawler.py
    python itviec_crawler.py --pages 5
    python itviec_crawler.py --pages 5 --output jobs.json
    python itviec_crawler.py --cdp-url http://localhost:9222 --pages 3

Requirements:
    pip install playwright
    playwright install chromium
"""

import os
import json
import time
import sys
import math
import random
import logging
import argparse
import concurrent.futures
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add project root to sys.path for cross-module imports
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from playwright.sync_api import sync_playwright, Page, Browser
from playwright.sync_api import TimeoutError as PlaywrightTimeout

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://itviec.com"
LIST_URL = f"{BASE_URL}/it-jobs"
CDP_URL = "http://localhost:9222"

DELAY_MIN = 2.0
DELAY_MAX = 4.0
PAGE_DELAY = 3.0

# Selectors — list page
SEL_JOB_CARD = "div.job-card"
ATTR_SLUG = "data-search--job-selection-job-slug-value"

# Selectors — detail page (verified từ DevTools)
SEL_TITLE = "h1.job-name, h1[class*='job-name'], h1"
SEL_COMPANY = "div.employer-name"
SEL_SALARY = "a.sign-in-view-salary"
SEL_LOCATIONS = "span.normal-text.text-rich-grey >> nth=0"
SEL_WORKING_METHOD = "span.normal-text.text-rich-grey >> nth=1"
SEL_SKILLS = "div:nth-child(3) > div:nth-child(2) > a.itag"
SEL_POSTED = "span.normal-text.text-rich-grey >> nth=2"
SEL_DESCRIPTION = "section.job-content"
SEL_EXPERTISE = "div:nth-child(4) > div:nth-child(2) > a.itag"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Browser Connection ────────────────────────────────────────────────────────


def launch_browser(playwright) -> Browser:
    """
    Launch Playwright's built-in Chromium headless (auto-browser mode).
    Used in Docker/CI environments where no external Chrome is available.
    Browser will be auto-closed when the crawl finishes.
    """
    log.info("Launching Chromium headless (auto-browser mode)...")
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
        ],
    )
    log.info("Chromium headless launched ✓")
    return browser


def connect_to_chrome(playwright, cdp_url: str) -> Browser:
    """
    Connect Playwright vào Chrome thật đang chạy qua CDP.
    Hỗ trợ kết nối xuyên Docker (host.docker.internal) bằng cách tự dò ws:// endpoint.
    """
    try:
        import urllib.request
        import urllib.parse

        # Nếu connect từ Docker qua host.docker.internal, Chrome sẽ chặn header Host.
        # Ta cần request bằng urllib với Host spoofing, lấy WebSocket URL thật rồi kết nối
        if cdp_url.startswith("http"):
            parsed = urllib.parse.urlparse(cdp_url)
            req = urllib.request.Request(
                f"{cdp_url}/json/version",
                headers={"Host": "localhost:9222"},  # Đánh lừa Chrome CDP
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                ws_url = data.get("webSocketDebuggerUrl")
                if ws_url:
                    # Ghi đè lại host.docker.internal nếu ws_url trả về 127.0.0.1 hoặc localhost
                    if (
                        parsed.hostname != "localhost"
                        and parsed.hostname != "127.0.0.1"
                    ):
                        ws_parsed = urllib.parse.urlparse(ws_url)
                        ws_url = ws_url.replace(ws_parsed.hostname, parsed.hostname)
                    cdp_url = ws_url

        browser = playwright.chromium.connect_over_cdp(cdp_url)
        log.info(f"Connected to Chrome via CDP at {cdp_url} ✓")
        return browser
    except Exception as e:
        raise ConnectionError(
            f"\n{'='*60}\n"
            f"Không connect được vào Chrome tại {cdp_url}\n\n"
            f"Chrome chưa được mở với CDP port. Chạy lệnh sau:\n\n"
            f"  Windows:\n"
            f'    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" '
            f'--remote-debugging-port=9222 --user-data-dir="C:\\chrome-cdp-profile"\n\n'
            f"  macOS:\n"
            f"    /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome "
            f"--remote-debugging-port=9222 --user-data-dir=/tmp/chrome-cdp-profile\n\n"
            f"  Linux:\n"
            f"    google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-cdp-profile\n\n"
            f"Sau đó vào itviec.com, vượt Cloudflare bằng tay, rồi chạy lại crawler.\n"
            f"{'='*60}\n"
            f"Chi tiết lỗi: {e}"
        )


def get_or_create_page(browser: Browser) -> Page:
    """
    Lấy tab đang mở trong Chrome hoặc tạo tab mới.
    Ưu tiên tab đang ở itviec.com nếu có.
    """
    contexts = browser.contexts
    if not contexts:
        return browser.new_context().new_page()

    context = contexts[0]
    pages = context.pages

    for p in pages:
        if "itviec.com" in p.url:
            log.info(f"Reusing existing itviec tab: {p.url}")
            return p

    return pages[0] if pages else context.new_page()


# ── Cloudflare Detection ──────────────────────────────────────────────────────


def is_cloudflare_blocked(page: Page) -> bool:
    """Phát hiện Cloudflare challenge page. Nếu bị chặn, cho user xử lý tay."""
    title = page.title().lower()
    url = page.url.lower()
    blocked = (
        "just a moment" in title
        or "checking your browser" in title
        or "cloudflare" in title
        or "/cdn-cgi/" in url
    )
    if blocked:
        log.error(
            "\nVẫn bị Cloudflare chặn!\n"
            "  → Chuyển sang Chrome, vượt challenge bằng tay,\n"
            "    rồi nhấn Enter để crawler tiếp tục...\n"
        )
        input("Nhấn Enter sau khi đã vượt Cloudflare xong...")
        time.sleep(2)
        title = page.title().lower()
        return "just a moment" in title or "cloudflare" in title
    return False


# ── Helpers ───────────────────────────────────────────────────────────────────


def random_delay(min_s: float = DELAY_MIN, max_s: float = DELAY_MAX):
    time.sleep(random.uniform(min_s, max_s))


def safe_text(page: Page, selector: str, default: str = "") -> str:
    try:
        el = page.query_selector(selector)
        return el.inner_text().strip() if el else default
    except Exception:
        return default


def safe_texts(page: Page, selector: str) -> list:
    try:
        els = page.query_selector_all(selector)
        return [el.inner_text().strip() for el in els if el.inner_text().strip()]
    except Exception:
        return []


def safe_attr(page: Page, selector: str, attr: str, default: str = "") -> str:
    try:
        el = page.query_selector(selector)
        return (el.get_attribute(attr) or default).strip() if el else default
    except Exception:
        return default


def parse_relative_date(text: str) -> str:
    """
    Convert relative time string to actual date (YYYY-MM-DD).

    Examples:
        '11 hours ago'  -> '2026-03-11'
        '49 days ago'   -> '2026-01-22'
        '2 months ago'  -> '2026-01-12'
    """
    if not text:
        return ""

    text_lower = text.strip().lower()

    unit_map = {
        "second": "seconds",
        "giây": "seconds",
        "minute": "minutes",
        "phút": "minutes",
        "hour": "hours",
        "giờ": "hours",
        "day": "days",
        "ngày": "days",
        "week": "weeks",
        "tuần": "weeks",
    }

    try:
        parts = text_lower.split()
        value = None
        unit_raw = ""

        for i, part in enumerate(parts):
            if part.isdigit():
                value = int(part)
                unit_raw = parts[i + 1] if i + 1 < len(parts) else ""
                break

        if value is None or not unit_raw:
            return text

        now = datetime.now(timezone.utc)

        if "month" in unit_raw or "tháng" in unit_raw:
            return (now - timedelta(days=value * 30)).strftime("%Y-%m-%d")

        if "year" in unit_raw or "năm" in unit_raw:
            return (now - timedelta(days=value * 365)).strftime("%Y-%m-%d")

        for keyword, td_arg in unit_map.items():
            if keyword in unit_raw:
                return (now - timedelta(**{td_arg: value})).strftime("%Y-%m-%d")

        return text

    except (ValueError, IndexError):
        return text


# ── Page Loading ──────────────────────────────────────────────────────────────


def load_page(page: Page, url: str, wait_time: float = 3.0) -> bool:
    """Navigate đến URL. Trả về False nếu bị Cloudflare hoặc lỗi."""
    try:
        page.set_default_timeout(60_000)
        page.goto(url, wait_until="load", timeout=60_000)

        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except PlaywrightTimeout:
            pass

        if wait_time > 0:
            time.sleep(wait_time)

        return not is_cloudflare_blocked(page)

    except Exception as e:
        log.error(f"Error loading {url}: {e}")
        return False


# ── Phase 1: Extract slugs ────────────────────────────────────────────────────


def get_job_slugs(page: Page) -> list:
    """Extract job slugs từ div.job-card elements."""
    cards = page.query_selector_all(SEL_JOB_CARD)
    slugs = []
    for card in cards:
        slug = card.get_attribute(ATTR_SLUG)
        if slug and slug.strip():
            slugs.append(slug.strip())
    return slugs


def get_max_pages(page: Page) -> int:
    """
    Auto-detect total number of pages from the pagination <nav>.

    ITviec pagination structure:
        <nav>
          <div class="page">...</div>   <!-- page 1 (current) -->
          <div class="page">...</div>   <!-- page 2 -->
          <div class="page">...</div>   <!-- ... (ellipsis) -->
          <div class="page"><a href="...?page=61...">61</a></div>  <!-- last -->
          <div class="page">...</div>   <!-- next button -->
        </nav>

    Strategy: find all <a> tags inside nav > div.page, parse their
    text content as integers, and return the maximum value found.
    """
    try:
        links = page.query_selector_all("nav div.page a")
        max_page = 1
        for link in links:
            text = link.inner_text().strip()
            if text.isdigit():
                max_page = max(max_page, int(text))

        log.info(f"Pagination detected: {max_page} total pages")
        return max_page
    except Exception as e:
        log.warning(f"Could not detect max pages ({e}), defaulting to 1")
        return 1


# ── Phase 2: Extract detail ───────────────────────────────────────────────────


def extract_job_detail(page: Page, slug: str):
    """Navigate đến detail page và extract structured data."""
    detail_url = f"{LIST_URL}/{slug}"
    log.info(f"  → {detail_url}")

    if not load_page(page, detail_url, wait_time=2.0):
        return None

    try:
        title = safe_text(page, SEL_TITLE)
        company = safe_text(page, SEL_COMPANY)
        salary = safe_text(page, SEL_SALARY)
        locations = safe_text(page, SEL_LOCATIONS)
        working_method = safe_text(page, SEL_WORKING_METHOD)
        skills = safe_texts(page, SEL_SKILLS)
        expertise = safe_text(page, SEL_EXPERTISE)
        description = safe_text(page, SEL_DESCRIPTION)

        posted_raw = safe_text(page, SEL_POSTED)
        posted_date = parse_relative_date(posted_raw)

        if not title:
            log.warning(f"  Missing title at {slug} — selectors có thể cần update")

        return {
            "job_id": f"itviec_{slug}",
            "title": title,
            "company": company,
            "salary": salary,
            "locations": locations,
            "working_method": working_method,
            "skills": skills,
            "expertise": expertise,
            "posted_date": posted_date,
            "description": description if description else "",
            "source": "itviec",
            "url": detail_url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.error(f"  Extract failed for {slug}: {e}")
        return None


def get_existing_job_ids() -> set:
    """Load existing job_ids from PostgreSQL to skip already crawled jobs."""
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "job_market")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")

    existing = set()
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'raw_jobs'
                );
            """
            )
            if cur.fetchone()[0]:
                cur.execute("SELECT job_id FROM raw_jobs")
                for row in cur.fetchall():
                    existing.add(row[0])
        conn.close()
        log.info(f"Loaded {len(existing)} existing job IDs from database to skip.")
    except Exception as e:
        log.warning(f"Could not load existing jobs from DB: {e}. Will crawl all.")
    return existing


def worker_crawl_chunk(
    slugs: list, cdp_url: str, auto_browser: bool, producer=None
) -> list:
    """
    Worker function to crawl a chunk of job slugs concurrently.
    Each thread creates its own sync_playwright instance and connects to the browser.
    """
    jobs = []
    with sync_playwright() as p:
        try:
            if auto_browser:
                browser = launch_browser(p)
            else:
                browser = connect_to_chrome(p, cdp_url)

            contexts = browser.contexts
            context = contexts[0] if contexts else browser.new_context()

            for slug in slugs:
                page = context.new_page()
                try:
                    job = extract_job_detail(page, slug)
                    if job:
                        jobs.append(job)
                        if producer:
                            producer.send_job(job)
                            log.info(f"    📤 Sent to Kafka (worker): '{job['title']}'")
                except Exception as e:
                    log.error(f"Error in worker for {slug}: {e}")
                finally:
                    page.close()
                random_delay(1.0, 2.0)

            if auto_browser:
                browser.close()

        except Exception as e:
            log.error(f"Worker initialization failed: {e}")

    return jobs


# ── Main ──────────────────────────────────────────────────────────────────────


def crawl(
    max_pages: int = 0,
    output_path: str = "itviec_jobs.json",
    cdp_url: str = CDP_URL,
    kafka_enabled: bool = True,
    kafka_broker: str = "localhost:29092",
    auto_browser: bool = False,
):
    """
    Main crawl function.
    Crawl ITviec job listings and optionally push each job to Kafka.

    Args:
        max_pages: Number of list pages to crawl.
                   0 = auto-detect from pagination (crawl ALL pages).
                   >0 = crawl exactly that many pages.
        output_path: Path to save the output JSON file.
        cdp_url: Chrome CDP URL for browser connection.
        kafka_enabled: Whether to send jobs to Kafka.
        kafka_broker: Kafka bootstrap server address.
        auto_browser: If True, launch headless Chromium internally
                      instead of connecting to external Chrome via CDP.
    """
    all_jobs = []
    producer = None

    existing_job_ids = get_existing_job_ids()

    # Initialize Kafka Producer if enabled
    if kafka_enabled:
        try:
            from kafka_app.kafka_producer import JobMarketKafkaProducer

            producer = JobMarketKafkaProducer(bootstrap_servers=kafka_broker)
            if producer.producer is None:
                log.warning(
                    "Kafka Producer failed to connect. Jobs will only be saved to file."
                )
                producer = None
        except ImportError:
            log.warning("kafka-python not installed. Jobs will only be saved to file.")
            producer = None

    with sync_playwright() as p:
        # Choose browser mode: auto-launch or connect to external CDP
        if auto_browser:
            browser = launch_browser(p)
            page = browser.new_page()
            log.info("Auto-browser: created new page")
        else:
            browser = connect_to_chrome(p, cdp_url)
            page = get_or_create_page(browser)

        # Load first page to detect total pages if auto-detect mode
        first_url = LIST_URL
        if not load_page(page, first_url, wait_time=3.0):
            log.error("Cannot load first page — aborting")
            return []

        # Auto-detect total pages from pagination if max_pages <= 0
        if max_pages <= 0:
            max_pages = get_max_pages(page)
            log.info(f"Auto-detect mode: will crawl all {max_pages} pages")
        else:
            detected = get_max_pages(page)
            if max_pages > detected:
                log.warning(
                    f"Requested {max_pages} pages but only {detected} exist. "
                    f"Capping to {detected}."
                )
                max_pages = detected
            log.info(f"Manual mode: will crawl {max_pages} pages")

        # Process page 1 (already loaded)
        slugs = get_job_slugs(page)
        log.info(f"\n[Page 1/{max_pages}] {first_url}")
        log.info(f"Found {len(slugs)} jobs")

        if slugs:
            # Lọc bỏ các jobs đã có trong Database
            original_len = len(slugs)
            slugs = [slug for slug in slugs if f"itviec_{slug}" not in existing_job_ids]
            if len(slugs) < original_len:
                log.info(
                    f"Skipped {original_len - len(slugs)} already crawled jobs. Remaining: {len(slugs)} jobs to extract."
                )

            if not slugs:
                log.info("Page 1 complete — all jobs already exist.")
            else:
                # Chia chunks để xử lý song song (Concurrency)
                num_workers = min(4, len(slugs))
                chunk_size = math.ceil(len(slugs) / num_workers)
                chunks = [
                    slugs[i : i + chunk_size] for i in range(0, len(slugs), chunk_size)
                ]
                log.info(f"Processing concurrently using {len(chunks)} threads...")

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=num_workers
                ) as executor:
                    futures = [
                        executor.submit(
                            worker_crawl_chunk, chunk, cdp_url, auto_browser, producer
                        )
                        for chunk in chunks
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        jobs = future.result()
                        all_jobs.extend(jobs)

                log.info(f"Page 1 complete — total collected: {len(all_jobs)} jobs")

        # Process remaining pages (2 .. max_pages)
        for page_num in range(2, max_pages + 1):
            list_url = f"{LIST_URL}?page={page_num}"
            log.info(f"\n[Page {page_num}/{max_pages}] {list_url}")

            if not load_page(page, list_url, wait_time=3.0):
                log.error("Cannot load page — stopping")
                break

            slugs = get_job_slugs(page)
            log.info(f"Found {len(slugs)} jobs")

            if not slugs:
                log.warning("No jobs found — end of listings or selector changed")
                break

            original_len = len(slugs)
            slugs = [slug for slug in slugs if f"itviec_{slug}" not in existing_job_ids]
            if len(slugs) < original_len:
                log.info(
                    f"Skipped {original_len - len(slugs)} already crawled jobs. Remaining: {len(slugs)} jobs."
                )

            if not slugs:
                log.info(f"Page {page_num} complete — all jobs already exist.")
                if page_num < max_pages:
                    random_delay(PAGE_DELAY, PAGE_DELAY + 1.5)
                continue

            num_workers = min(4, len(slugs))
            chunk_size = math.ceil(len(slugs) / num_workers)
            chunks = [
                slugs[i : i + chunk_size] for i in range(0, len(slugs), chunk_size)
            ]
            log.info(f"Processing concurrently using {len(chunks)} threads...")

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_workers
            ) as executor:
                futures = [
                    executor.submit(
                        worker_crawl_chunk, chunk, cdp_url, auto_browser, producer
                    )
                    for chunk in chunks
                ]
                for future in concurrent.futures.as_completed(futures):
                    jobs = future.result()
                    all_jobs.extend(jobs)

            log.info(
                f"Page {page_num} complete — total collected: {len(all_jobs)} jobs"
            )

            if page_num < max_pages:
                random_delay(PAGE_DELAY, PAGE_DELAY + 1.5)

        # Close browser if we launched it (auto-browser mode)
        if auto_browser:
            browser.close()
            log.info("Auto-browser: Chromium closed ✓")
        else:
            log.info("Crawler done — external Chrome still running")

    # Flush and close Kafka Producer
    if producer:
        producer.flush()
        producer.close()
        log.info(f"Kafka: all {len(all_jobs)} jobs flushed to topic 'raw_jobs'")

    # Dedup theo URL
    seen, unique_jobs = set(), []
    for job in all_jobs:
        if job["url"] not in seen:
            seen.add(job["url"])
            unique_jobs.append(job)

    Path(output_path).write_text(
        json.dumps(unique_jobs, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"\nDone. Saved {len(unique_jobs)} unique jobs → {output_path}")
    return unique_jobs


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ITviec Crawler — CDP or Auto-browser mode"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=0,
        help="Number of pages to crawl. 0 = auto-detect all pages (default: 0)",
    )
    parser.add_argument(
        "--output", type=str, default="itviec_jobs.json", help="Output JSON file"
    )
    parser.add_argument(
        "--cdp-url",
        type=str,
        default=CDP_URL,
        help=f"Chrome CDP URL (default: {CDP_URL})",
    )
    parser.add_argument(
        "--no-kafka", action="store_true", help="Disable Kafka ingestion"
    )
    parser.add_argument(
        "--kafka-broker",
        type=str,
        default="localhost:29092",
        help="Kafka bootstrap server (default: localhost:29092)",
    )
    parser.add_argument(
        "--auto-browser",
        action="store_true",
        help="Launch headless Chromium internally (no external Chrome needed)",
    )
    args = parser.parse_args()

    crawl(
        max_pages=args.pages,
        output_path=args.output,
        cdp_url=args.cdp_url,
        kafka_enabled=not args.no_kafka,
        kafka_broker=args.kafka_broker,
        auto_browser=args.auto_browser,
    )
