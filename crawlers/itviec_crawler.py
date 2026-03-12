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

import json
import time
import sys
import random
import logging
import argparse
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


# ── CDP Connection ────────────────────────────────────────────────────────────


def connect_to_chrome(playwright, cdp_url: str) -> Browser:
    """
    Connect Playwright vào Chrome thật đang chạy qua CDP.
    Raise lỗi rõ ràng nếu Chrome chưa được mở với --remote-debugging-port.
    """
    try:
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


def safe_texts(page: Page, selector: str) -> list[str]:
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


def get_job_slugs(page: Page) -> list[str]:
    """Extract job slugs từ div.job-card elements."""
    cards = page.query_selector_all(SEL_JOB_CARD)
    slugs = []
    for card in cards:
        slug = card.get_attribute(ATTR_SLUG)
        if slug and slug.strip():
            slugs.append(slug.strip())
    return slugs


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
            "title": title,
            "company": company,
            "salary": salary,
            "locations": locations,
            "working_method": working_method,
            "skills": skills,
            "expertise": expertise,
            "posted_date": posted_date,
            "description": description[:2000] if description else "",
            "source": "itviec",
            "url": detail_url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.error(f"  Extract failed for {slug}: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────────


def crawl(
    max_pages: int = 3,
    output_path: str = "itviec_jobs.json",
    cdp_url: str = CDP_URL,
    kafka_enabled: bool = True,
    kafka_broker: str = "localhost:29092",
):
    """
    Main crawl function.
    Crawl ITviec job listings and optionally push each job to Kafka.

    Args:
        max_pages: Number of list pages to crawl.
        output_path: Path to save the output JSON file.
        cdp_url: Chrome CDP URL for browser connection.
        kafka_enabled: Whether to send jobs to Kafka.
        kafka_broker: Kafka bootstrap server address.
    """
    all_jobs = []
    producer = None

    # Initialize Kafka Producer if enabled
    if kafka_enabled:
        try:
            from kafka.kafka_producer import JobMarketKafkaProducer
            producer = JobMarketKafkaProducer(bootstrap_servers=kafka_broker)
            if producer.producer is None:
                log.warning("Kafka Producer failed to connect. Jobs will only be saved to file.")
                producer = None
        except ImportError:
            log.warning("kafka-python not installed. Jobs will only be saved to file.")
            producer = None

    with sync_playwright() as p:
        # Connect vào Chrome thật — không launch browser mới
        browser = connect_to_chrome(p, cdp_url)
        page = get_or_create_page(browser)

        for page_num in range(1, max_pages + 1):
            list_url = LIST_URL if page_num == 1 else f"{LIST_URL}?page={page_num}"
            log.info(f"\n[Page {page_num}/{max_pages}] {list_url}")

            if not load_page(page, list_url, wait_time=3.0):
                log.error("Dừng — không load được trang danh sách")
                break

            slugs = get_job_slugs(page)
            log.info(f"Found {len(slugs)} jobs")

            if not slugs:
                log.warning("Không có jobs — selector có thể đã thay đổi")
                break

            for i, slug in enumerate(slugs, 1):
                log.info(f"  [{i}/{len(slugs)}] {slug}")
                job = extract_job_detail(page, slug)
                if job:
                    all_jobs.append(job)
                    log.info(f"    ✓ {job['title']} @ {job['company']}")

                    # Push to Kafka immediately after extraction
                    if producer:
                        if producer.send_job(job):
                            log.info(f"    📤 Sent to Kafka: '{job['title']}' ({job['source']})")
                        else:
                            log.warning(f"    ⚠ Failed to send to Kafka: '{job['title']}'")

                random_delay()

            log.info(f"Page {page_num} complete — total: {len(all_jobs)} jobs")

            if page_num < max_pages:
                random_delay(PAGE_DELAY, PAGE_DELAY + 1.5)

        # KHÔNG gọi browser.close() — Chrome thật vẫn tiếp tục chạy
        log.info("Crawler done — Chrome vẫn đang chạy bình thường")

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
        description="ITviec Crawler — CDP connect to real Chrome"
    )
    parser.add_argument(
        "--pages", type=int, default=3, help="Số trang cần crawl (default: 3)"
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
    args = parser.parse_args()

    crawl(
        max_pages=args.pages,
        output_path=args.output,
        cdp_url=args.cdp_url,
        kafka_enabled=not args.no_kafka,
        kafka_broker=args.kafka_broker,
    )
