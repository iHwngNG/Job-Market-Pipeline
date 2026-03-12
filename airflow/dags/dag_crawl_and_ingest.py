"""
Airflow DAG: dag_crawl_and_ingest
---------------------------------
Schedule: Every day at 7:00 AM (Asia/Ho_Chi_Minh timezone)
Task: Run ITviec Crawler → Extract job details → Push to Kafka topic 'raw_jobs'

Flow:
  1. BashOperator triggers the itviec_crawler.py script
  2. Crawler connects to Chrome via CDP, extracts job listings
  3. Each extracted job is immediately sent to Kafka by the integrated Producer
  4. Producer logs a notification for every job sent: "📤 Sent to Kafka: '<title>'"
  5. All jobs are also saved to itviec_jobs.json as backup
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── DAG Default Arguments ─────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_crawl_and_ingest",
    default_args=default_args,
    description="Crawl ITviec jobs daily at 7AM and push to Kafka",
    # Chạy lúc 7h sáng giờ Việt Nam (UTC+7 → 0h UTC)
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 3, 12),
    catchup=False,
    tags=["crawl", "itviec", "kafka", "ingestion"],
) as dag:

    # ── Task: Run ITviec Crawler ──────────────────────────────────────────────
    # BashOperator gọi script crawler với các tham số:
    #   --pages 5        : Crawl 5 trang (khoảng 100 jobs)
    #   --kafka-broker   : Kafka broker address (trong Docker network)
    #   --cdp-url        : Chrome CDP URL
    #   --output         : File JSON backup

    # ── Task 1: Ensure Chrome CDP is running ──────────────────────────────────
    launch_chrome_CDP = BashOperator(
        task_id="ensure_chrome_cdp",
        bash_command='powershell.exe -File "utils/launch_chrome_CDP_cdp.ps1"',
    )

    # ── Task 2: Run ITviec Crawler ──────────────────────────────────────────────
    crawl_itviec = BashOperator(
        task_id="crawl_itviec_to_kafka",
        bash_command=(
            "cd /opt/airflow && "
            "python crawlers/itviec_crawler.py "
            "--pages 5 "
            "--kafka-broker localhost:29092 "
            "--cdp-url http://host.docker.internal:9222 "
            "--output /opt/airflow/data/itviec_jobs_{{ ds }}.json"
        ),
    )

    launch_chrome >> crawl_itviec
