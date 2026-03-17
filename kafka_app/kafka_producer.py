import json
import logging
import time
from typing import Dict, Any, Optional
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class JobMarketKafkaProducer:
    """
    Kafka Producer wrapper wrapper để đẩy dữ liệu crawl được (Job listings) vào Kafka.
    """
    
    def __init__(self, bootstrap_servers: str = "localhost:29092", topic: str = "raw_jobs", retries: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.retries = retries
        self.producer = self._connect_kafka_producer()

    def _connect_kafka_producer(self) -> Optional[KafkaProducer]:
        """Thử kết nối đến Kafka broker với cơ chế retry."""
        _producer = None
        for attempt in range(1, self.retries + 1):
            try:
                log.info(f"Attempting to connect to Kafka (Attempt {attempt}/{self.retries})...")
                _producer = KafkaProducer(
                    bootstrap_servers=[self.bootstrap_servers],
                    # Serialize dictionary thành chuỗi JSON dạng byte, hỗ trợ tiếng Việt (ensure_ascii=False)
                    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
                    api_version=(0, 10, 1),
                    acks='all',  # Đảm bảo record được ghi vào tất cả replicas
                    retries=5    # Kafka client tự động retry nều thất bại tạm thời
                )
                log.info("Successfully connected to Kafka broker! ✓")
                return _producer
            except NoBrokersAvailable:
                log.error(f"Kafka broker not available at {self.bootstrap_servers}.")
                if attempt < self.retries:
                    time.sleep(3)
            except Exception as e:
                log.error(f"Failed to connect to Kafka: {e}")
                if attempt < self.retries:
                    time.sleep(3)
        return None

    def send_job(self, job_data: Dict[str, Any]) -> bool:
        """
        Gửi một record công việc (job) vào Kafka topic.
        """
        if not self.producer:
            log.error("Kafka Producer is not initialized. Cannot send message.")
            return False

        try:
            # Chọn 'url' làm key để đảm bảo các job giống nhau sẽ rơi vào cùng 1 partition (hữu ích khi có 3 partitions)
            key = str(job_data.get('url', '')).encode('utf-8') if job_data.get('url') else None
            
            # Gửi bất đồng bộ (async send)
            future = self.producer.send(
                self.topic, 
                value=job_data,
                key=key
            )
            
            # Wait for confirmation (đảm bảo tin nhắn đã vào topic thành công)
            record_metadata = future.get(timeout=10)
            
            log.debug(f"Pushed job '{job_data.get('title')}' "
                      f"to topic {record_metadata.topic} "
                      f"partition {record_metadata.partition} "
                      f"offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            log.error(f"Failed to send job data to Kafka: {e}")
            return False

    def flush(self):
        """Đảm bảo tất cả các tin nhắn đang chờ trong buffer được gửi đi."""
        if self.producer:
            log.info("Flushing Kafka Producer...")
            self.producer.flush()

    def close(self):
        """Đóng kết nối Kafka."""
        if self.producer:
            log.info("Closing Kafka Producer...")
            self.producer.close()

# Khối IF này dùng để chạy test nhanh script
if __name__ == "__main__":
    producer = JobMarketKafkaProducer()
    
    test_message = {
        "title": "Data Engineer (Fresher)",
        "company": "Tech Corp VN",
        "salary": "Negotiable",
        "locations": "Hồ Chí Minh",
        "skills": ["Python", "Spark", "Kafka", "PostgreSQL"],
        "posted_date": "2026-03-12",
        "source": "test_script",
        "url": "https://example.com/job/data-engineer",
        "crawled_at": "2026-03-12T09:00:00+00:00"
    }

    if producer.send_job(test_message):
        log.info(f"Test message '{test_message['title']}' successfully sent!")
    
    producer.flush()
    producer.close()
