# Progress Logs

| ID | Date | Time | Description |
|---|---|---|---|
| 1 | 2026-03-12 | 01:30:00 | Initialize project structure based on PRD. Created `docs/logs.md` and `docs/error_debugging_logs.md`. |
| 2 | 2026-03-12 | 01:35:00 | Created directory structure and placeholder files for crawlers, spark_jobs, dbt, airflow, and dashboard modules. |
| 3 | 2026-03-12 | 01:38:00 | Set up GitHub Actions for CI (linting and testing) and added `.gitignore` to protect sensitive files and avoid redundant assets. |
| 4 | 2026-03-12 | 01:40:00 | Committed and pushed initial project structure and documentation to GitHub. |
| 5 | 2026-03-12 | 01:45:00 | Set up `docker-compose.yml` and `.env.example` for Kafka, PostgreSQL, Airflow, and Spark. Included Kafka-UI for easy monitoring. |
| 6 | 2026-03-12 | 02:36:00 | Implemented `get_job_slugs_from_response` in `itviec_crawler.py` to extract job slugs from ITviec job cards using Scrapy selectors. |
| 7 | 2026-03-12 | 02:39:00 | Added `get_html_with_selenium` function to `itviec_crawler.py` to handle JavaScript rendering for crawling using Selenium WebDriver. |
| 8 | 2026-03-12 | 02:56:00 | Switched from Selenium to Playwright in `itviec_crawler.py` for better reliability and performance in handling JS-rendered content. Updated `get_html_with_playwright` function. |
| 9 | 2026-03-12 | 05:15:00 | Fixed Playwright `TimeoutError` in `itviec_crawler.py` by switching wait condition to `load` and increasing the timeout limit. |
| 10 | 2026-03-12 | 05:24:00 | Rewrote `itviec_crawler.py` to use slug-based navigation: extract slugs from list page, then visit each `itviec.com/it-jobs/{slug}` detail page individually. |
