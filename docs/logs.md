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
| 11 | 2026-03-12 | 09:15:00 | Analyzed PRD and finalized technical decisions for Kafka integration. |
| 12 | 2026-03-12 | 09:20:00 | Implemented professional Kafka initialization using a `init-topics.sh` script and a dedicated `kafka-init` service in `docker-compose.yml`. |
| 13 | 2026-03-12 | 09:25:00 | Created `kafka/kafka_producer.py` wrapper class `JobMarketKafkaProducer` to send crawler output strings to Kafka topic `raw_jobs`. |
| 14 | 2026-03-12 | 22:15:00 | Integrated Kafka Producer into `itviec_crawler.py` main loop. Each crawled job is now pushed to Kafka immediately with a log notification. Added `--no-kafka` and `--kafka-broker` CLI args. |
| 15 | 2026-03-12 | 22:20:00 | Created Airflow DAG `dag_crawl_and_ingest.py` scheduled at 7:00 AM daily (UTC+7). Triggers ITviec crawler with Kafka ingestion enabled. |
| 16 | 2026-03-12 | 22:30:00 | Created `utils/launch_chrome_cdp.ps1` to automate Chrome startup. Updated Airflow DAG to ensure Chrome is ready before crawling. |
| 17 | 2026-03-13 | 16:40:00 | Created `kafka/kafka_consumer.py` to consume messages from Kafka and insert them into the `raw_jobs_bronze` table in PostgreSQL. Handle duplicate jobs using UPSERT. |
| 18 | 2026-03-13 | 16:40:00 | Updated `itviec_crawler.py` to generate `job_id` correctly and format features like `description` directly optimized for downstrea Kafka analytics. |
| 19 | 2026-03-14 | 02:47:00 | Added `kafka-consumer` service to `docker-compose.yml` to run the ingestion script nicely in the background alongside Kafka and PostgreSQL. |
| 20 | 2026-03-17 | 03:50:00 | Decided to adopt a Lean Pipeline architecture (Raw -> Analytics). Updated `kafka_consumer.py` to rename `raw_jobs_bronze` to simply `raw_jobs`. Renamed the PySpark script file to `process_analytics.py`. |
| 21 | 2026-03-17 | 04:40:00 | Designed and implemented `spark_jobs/process_analytics.py` for Lean ETL. Includes PySpark UDFs for parsing salary strings (min, max, currency) and smartly merging dedup skills from JSON arrays + parsing text description. Uses `local[1]` driver optimizations + automatic JDBC PostgreSQL `.jar` fetching. |
| 22 | 2026-03-17 | 04:47:00 | Massively expanded the `COMMON_SKILLS` dictionary in `process_analytics.py` for PySpark NLP description extraction. Added deep coverage for Data Engineering (Redshift, Databricks, Flink), Data Analysis (Tableau, PowerBI), Machine Learning (Pandas, XGBoost), and GenAI/LLM stack (Langchain, HuggingFace, RAG). |
| 23 | 2026-03-17 | 05:00:00 | Added `standardize_role` UDF to PySpark — maps raw titles to 20+ standardized roles (Data Engineer, ML Engineer, Frontend Developer, etc.) using keyword priority matching. Added `standardize_level` UDF — extracts experience level (Intern→Manager) from title + expertise fields with year-range fallback. |
| 24 | 2026-03-17 | 05:05:00 | Built complete dbt Module 3: `dbt_project.yml`, `profiles.yml`, `sources.yml` pointing to `analytics_jobs`. Created `stg_jobs.sql` (staging + avg_salary), and 4 mart views: `dim_skills` (skill demand by role/location), `dim_companies` (top hiring), `fct_salary_trends` (salary stats by role/level), `fct_market_overview` (KPIs + breakdowns). All materialized as views for zero storage overhead. |
| 25 | 2026-03-17 | 05:05:00 | Created `schema.yml` with comprehensive data quality tests: `unique` + `not_null` on `job_id`, `accepted_values` on `level` and `source` columns, `not_null` on all key metric columns across all models. |
| 26 | 2026-03-17 | 05:25:00 | Built Streamlit Dashboard (Module 4) in `dashboard/app.py`. Features: dark theme with gradient KPI cards, 6 Plotly charts (Top Skills, Top Roles, Level Distribution, Salary by Role, Top Companies, Salary Range by Level), sidebar filters (Location, Role), detailed data table. Added `.streamlit/config.toml` for premium dark mode styling. All queries target dbt views via `public_staging` and `public_marts` schemas. |
| 27 | 2026-03-17 | 05:35:00 | Created `dag_transform_and_model.py` (DAG 2) chaining: SqlSensor (check raw data) → PySpark ETL → dbt run → dbt test. Scheduled 30 min after crawl DAG. |
| 28 | 2026-03-17 | 05:40:00 | Created `dashboard/Dockerfile` and `dashboard/requirements.txt` for containerized Streamlit deployment. |
| 29 | 2026-03-17 | 05:45:00 | Rewrote `docker-compose.yml` for full pipeline orchestration. Mounted crawlers/spark_jobs/dbt/kafka/utils into Airflow. Added dbt-core, pyspark, playwright to Airflow pip deps. Added `dashboard` service. Auto-creates Airflow DB connection `postgres_job_market` for SqlSensor. Created `infra/postgres/init-multi-db.sh` to bootstrap both `job_market` and `airflow` databases in a single Postgres container. |
| 30 | 2026-03-17 | 05:56:00 | Created proper Docker packaging with pre-built images (zero runtime pip install). Created `Dockerfile.airflow` (base: apache/airflow:2.7.1 + Java/PySpark/dbt/Playwright), `Dockerfile.consumer` (base: python:3.10-slim + psycopg2/kafka-python), updated `dashboard/Dockerfile`. Created `requirements-airflow.txt` and `requirements-consumer.txt` with pinned versions. Rewrote `docker-compose.yml` to use `build:` directives for 3 custom images: `jobmarket-airflow`, `jobmarket-consumer`, `jobmarket-dashboard`. |
| 31 | 2026-03-17 | 06:40:00 | Fixed Airflow init failure: replaced `init-multi-db.sh` (CRLF issue on Windows) with `init-airflow-db.sql`. Removed `version: '3.8'` from docker-compose. Rewrote `dashboard/app.py` to query `analytics_jobs` directly (no dbt view dependency). Added `table_exists()` check with user-facing message when no data. |
| 32 | 2026-03-17 | 07:00:00 | Fixed Airflow `ModuleNotFoundError`: removed `user: "${AIRFLOW_UID:-1000:0}"` from docker-compose (was overriding the default UID 50000 causing Python path issues). Removed `sqlalchemy==2.0.25` from `requirements-airflow.txt` (conflicted with Airflow's bundled version). Added `concat_ws` import to `process_analytics.py` for skills array serialization. |
| 33 | 2026-03-17 | 07:48:00 | DAG chaining: Added `TriggerDagRunOperator` to end of DAG 1 (`dag_crawl_and_ingest`) to auto-trigger DAG 2. Changed DAG 2 (`dag_transform_and_model`) schedule from `30 0 * * *` to `None` (event-driven only). Added 60s auto-refresh to Dashboard via meta tag + reduced query cache TTL from 300s to 60s. |
| 34 | 2026-03-17 | 08:05:00 | Added auto-detect pagination to `itviec_crawler.py`. New `get_max_pages()` function reads `nav div.page a` elements to find the last page number. Default `--pages` changed from 3 to 0 (auto-detect all pages). Also validates: if user requests more pages than exist, caps to actual max. Removed hardcoded `--pages 5` from DAG 1. |
| 35 | 2026-03-17 | 08:16:00 | Fixed `TypeError: 'type' object is not subscriptable` in Docker (Python 3.8): changed `list[str]` type hints to `list` in `safe_texts()` and `get_job_slugs()`. Fixed CDP cross-Docker connection by spoofing `Host: localhost` header when fetching WS URL from `/json/version`. |
| 36 | 2026-03-17 | 08:24:00 | Created `Makefile` with commands: `up`, `down`, `restart`, `build-all`, `clean`, `logs-*`, `test-crawl`. Created comprehensive `README.md` with architecture overview, prerequisites, quick start guide, and command reference. |
| 37 | 2026-03-17 | 08:37:00 | Added `--auto-browser` mode to crawler: launches headless Chromium via Playwright internally (no external Chrome needed). Updated `Dockerfile.airflow` to install `xvfb` and `playwright install-deps chromium` for system libs. Rewrote DAG 1 to use `--auto-browser` + `--kafka-broker kafka:9092` (Docker internal). Removed impossible `powershell.exe` task from DAG. |
| 38 | 2026-03-17 | 09:40:00 | Reverted DAG 1 from `--auto-browser` back to `--cdp-url http://host.docker.internal:9222`. Reason: Headless Chromium spawned inside Docker instantly gets blocked by Cloudflare. To bypass bot protection, it must hook into the real Chrome instance running on the Windows host. |
| 39 | 2026-03-17 | 09:44:00 | Integrated PowerShell `launch_chrome_cdp.ps1` script directly into the `make up` target. Simplified Windows user onboarding significantly: running `make up` now silently spawns the Chrome debug profile alongside the Docker environment automatically. |
| 40 | 2026-03-17 | 10:25:00 | Boosted Crawler Performance: Refactored `itviec_crawler.py` to use `concurrent.futures.ThreadPoolExecutor`. Job slugs are now split into batches and processed concurrently using 4 parallel workers. Each thread connects to the CDP session, drastically improving crawling speed from O(N) sequential to O(N/4). |
| 41 | 2026-03-17 | 10:35:00 | Added database check before scraping URLs in `itviec_crawler.py`. The script now connects to Postgres, fetches all existing `job_id`s from the `raw_jobs` table, and filters out already crawled URLs (slugs) directly in memory *before* passing them to the worker threads. This avoids re-downloading existing pages, saving massive time on subsequent runs. |


| 42 | 2026-03-17 | 15:52:00 | Updated Python environment in dashboard/Dockerfile from version 3.10-slim to 3.9-slim to ensure all Docker environments use Python 3.9. Verified Dockerfile.airflow and Dockerfile.consumer are already using Python 3.9. |

| 43 | 2026-03-17 | 16:15:00 | Fixed `dbt: command not found` in `dag_transform_and_model.py` by explicitly invoking dbt using `python3 -m dbt` instead of the raw dbt executable in `BashOperator` commands. |

| 44 | 2026-03-17 | 16:19:00 | Fixed `No module named dbt.__main__` in `dag_transform_and_model.py` by exporting Airflow user's pip executable path `/home/airflow/.local/bin` to `` inside `BashOperator` commands instead of using python module module invocation. |

| 45 | 2026-03-17 | 16:24:00 | Fixed type mismatch error in dbt model `dim_skills.sql` by converting string into an array using `string_to_array(standardized_skills, ',')` before passing it to `UNNEST()`, appending `TRIM()` to clean string. |

| 46 | 2026-03-17 | 16:30:00 | Fixed TypeError in `dashboard/app.py` plotly figure layout override. Extracted explicitly assigned `yaxis` params from `update_layout(**PLOTLY_LAYOUT)` to independent calls using `fig.update_yaxes()` avoiding multiple values argument error. |

| 47 | 2026-03-17 | 16:36:00 | Fixed dashboard container not picking up python hot-reloads by adding volume mount `./dashboard:/app` to `docker-compose.yml`, ensuring changes mapped out to container instantly. Re-started dashboard container block to apply. |

| 48 | 2026-03-17 | 16:50:00 | Clean up unused or temporary files in the repository (e.g. `crawler.log`, `crawlers/test_single.py`, raw output JSONs) to ensure a clean codebase. Intentionally kept `topcv_crawler.py` as requested. |

| 49 | 2026-03-17 | 17:30:00 | Completely rewrote README.md to be a professional project documentation. Added clear architecture & workflow explanation, module/folder definitions, prerequisite instructions, and Makefile command guide. |

