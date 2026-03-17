"""
Airflow DAG: dag_transform_and_model
-------------------------------------
Schedule: None (triggered automatically by dag_crawl_and_ingest or manually)
Tasks:
  1. Wait for raw data in PostgreSQL (sensor)
  2. Run PySpark ETL: raw_jobs -> analytics_jobs
  3. Run dbt: create staging + mart views
  4. Run dbt tests: validate data quality

Flow:
  [Check Data] -> [PySpark Transform] -> [dbt Run] -> [dbt Test]
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.sql import SqlSensor

# ── DAG Default Arguments ─────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_transform_and_model",
    default_args=default_args,
    description="PySpark ETL + dbt modeling — triggered by crawl DAG",
    # No fixed schedule — only runs when triggered by DAG 1 or manually
    schedule_interval=None,
    start_date=datetime(2026, 3, 12),
    catchup=False,
    tags=["transform", "pyspark", "dbt", "modeling"],
) as dag:

    # ── Task 1: Check if raw_jobs has new data ────────────────────────────────
    check_raw_data = SqlSensor(
        task_id="check_raw_data_exists",
        conn_id="postgres_job_market",
        sql="SELECT COUNT(*) FROM raw_jobs WHERE crawled_at >= NOW() - INTERVAL '2 days'",
        mode="poke",
        poke_interval=60,
        timeout=600,
    )

    # ── Task 2: Run PySpark ETL ───────────────────────────────────────────────
    run_pyspark = BashOperator(
        task_id="run_pyspark_transform",
        bash_command=(
            "export PYSPARK_PYTHON=python3 && "
            "export PYSPARK_DRIVER_PYTHON=python3 && "
            "cd /opt/airflow && "
            "python3 spark_jobs/process_analytics.py"
        ),
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "job_market",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
        },
    )

    # ── Task 3: Run dbt models ────────────────────────────────────────────────
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "export PATH=/home/airflow/.local/bin:$PATH && "
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --project-dir ."
        ),
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "job_market",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
        },
    )

    # ── Task 4: Run dbt tests ─────────────────────────────────────────────────
    test_dbt = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            "export PATH=/home/airflow/.local/bin:$PATH && "
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir . --project-dir ."
        ),
        env={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "job_market",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
        },
    )

    # ── Pipeline Flow ─────────────────────────────────────────────────────────
    check_raw_data >> run_pyspark >> run_dbt >> test_dbt
