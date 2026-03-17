"""
Airflow DAG: dag_crawl_and_ingest
---------------------------------
Schedule: Every day at 7:00 AM (Asia/Ho_Chi_Minh timezone)
Task: Run ITviec Crawler → Push to Kafka → Auto-trigger DAG 2

Flow:
  1. Crawler launches headless Chromium internally (--auto-browser)
  2. Auto-detects total pages from pagination, crawls all listings
  3. Each extracted job is immediately sent to Kafka by the Producer
  4. After crawl completes, trigger dag_transform_and_model automatically
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    description="Crawl ITviec jobs daily at 7AM, push to Kafka, then trigger transform",
    # Run at 7:00 AM Vietnam time (UTC+7 → 0:00 UTC)
    schedule_interval="0 0 * * *",
    start_date=datetime(2026, 3, 12),
    catchup=False,
    tags=["crawl", "itviec", "kafka", "ingestion"],
) as dag:

    # ── Task 1: Run ITviec Crawler (CDP mode) ─────────────────────────────────
    crawl_itviec = BashOperator(
        task_id="crawl_itviec_to_kafka",
        bash_command=(
            "cd /opt/airflow && "
            "python crawlers/itviec_crawler.py "
            "--cdp-url http://host.docker.internal:9222 "
            "--kafka-broker kafka:9092 "
            "--output /opt/airflow/data/itviec_jobs_{{ ds }}.json"
        ),
    )

    # ── Task 2: Auto-trigger DAG 2 after crawl completes ─────────────────────
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="dag_transform_and_model",
        wait_for_completion=False,  # Fire-and-forget, don't block
    )

    # ── Pipeline Flow ─────────────────────────────────────────────────────────
    # Crawl → Trigger Transform DAG
    crawl_itviec >> trigger_transform
