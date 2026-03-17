import os
import json
import logging
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def create_table_if_not_exists(cursor):
    """
    Tạo table chứa raw data (raw_jobs) trong PostgreSQL.
    Các trường JSON phức tạp được lưu vào raw_data (kiểu JSONB) để dễ dàng
    bóc tách bằng PySpark sau này nhưng không mất thông tin.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_jobs (
        id SERIAL PRIMARY KEY,
        job_id VARCHAR(255) UNIQUE,
        source VARCHAR(50),
        raw_data JSONB,
        crawled_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_raw_jobs_job_id ON raw_jobs(job_id);
    CREATE INDEX IF NOT EXISTS idx_raw_jobs_source ON raw_jobs(source);
    """
    cursor.execute(create_table_query)
    log.info("Checked/Created table 'raw_jobs'")

def consume_jobs():
    """
    Consumer đọc dữ liệu từ Kafka topic 'raw_jobs' và INSERT vào Postgres.
    Sử dụng ON CONFLICT (Upsert) để đảm bảo Job Crawler dù chạy nhiều lần
    vẫn không sinh ra dữ liệu trùng id.
    """
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:29092")
    topic = "raw_jobs"

    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "job_market")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")

    log.info(f"Connecting to Kafka at {kafka_broker}...")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_broker,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        group_id="bronze_ingestion_group",
        auto_offset_reset="earliest",  # Đọc từ đầu nếu chưa có commit mark
        enable_auto_commit=True
    )
    
    log.info(f"Connecting to DB: {db_host}:{db_port}/{db_name}...")
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        log.error(f"Failed to connect to database: {e}")
        return

    create_table_if_not_exists(cursor)

    log.info(f"Started consuming from topic '{topic}'...")

    try:
        for message in consumer:
            job = message.value
            if not job:
                continue
                
            job_id = job.get("job_id", "unknown_id")
            source = job.get("source", "unknown_source")
            crawled_at = job.get("crawled_at")

            # Upsert (Insert, nếu đã tồn tại thì Update thông tin mới nhất)
            insert_query = """
            INSERT INTO raw_jobs (job_id, source, raw_data, crawled_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE 
            SET raw_data = EXCLUDED.raw_data,
                crawled_at = EXCLUDED.crawled_at;
            """
            try:
                cursor.execute(insert_query, (job_id, source, Json(job), crawled_at))
                log.info(f"Saved/Updated job to DB: {job_id} ({source})")
            except Exception as e:
                log.error(f"Error saving job {job_id}: {e}")
                
    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        log.info("Connections closed.")

if __name__ == "__main__":
    consume_jobs()
