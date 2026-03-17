# Vietnam Job Market Intelligence Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Playwright](https://img.shields.io/badge/Playwright-2EAD33?style=for-the-badge&logo=Playwright&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)

<img width="1272" height="825" alt="jobmarketpipeline_preview" src="https://github.com/user-attachments/assets/01e89b8c-6e26-46e0-b095-ce604b743773" />

A robust, end-to-end data pipeline to crawl, ingest, process, model, and visualize IT job market data in Vietnam. The project is fully containerized using Docker and orchestrated using Apache Airflow.

⚠️ *The project is developed with the assistance of AI Agents* ⚠️

---

## 🏗️ Architecture & Workflow

The pipeline is designed with a Lean Architecture (Raw -> Analytics) to ensure real-time performance and scalability. Here is the step-by-step data flow:

1. **Intelligent Crawling:** 
   - The crawler (`crawlers/itviec_crawler.py`) is triggered via Airflow. It uses **Playwright** via Chrome DevTools Protocol (CDP) to render JavaScript-heavy job listings.
   - **Optimization:** Before scraping, it connects to PostgreSQL to fetch existing `job_id`s in the database and filters out already crawled URLs in-memory. This prevents redundant scraping and drastically speeds up the process.
   - Scraped job data is immediately sent to a **Kafka** topic (`raw_jobs`) using the custom Kafka Producer. A backup JSON is also saved locally to the `data/` folder.

2. **Real-time Ingestion:**
   - A dedicated **Kafka Consumer** container continuously listens to the `raw_jobs` topic.
   - It consumes the messages and executes an `UPSERT` operation into the `raw_jobs` table in **PostgreSQL**.

3. **Transformation & ETL (PySpark):**
   - Airflow triggers the PySpark job (`spark_jobs/process_analytics.py`) to read from the `raw_jobs` table.
   - The ETL process parses salaries, normalizes currencies, extracts technical skills from job descriptions using NLP rules, standardizes job roles (e.g., Data Engineer, ML Engineer), and identifies seniority levels.
   - Results are written back to PostgreSQL into the `analytics_jobs` table.

4. **Data Modeling & Quality Validation (dbt):**
   - Next, **dbt** builds staging and mart views on top of `analytics_jobs` (e.g., `dim_skills`, `dim_companies`, `fct_salary_trends`) to serve analytics queries efficiently without duplicating storage.
   - `dbt test` runs to enforce data quality rules (unique constraints, non-null checks, accepted values).

5. **Visualization (Streamlit):**
   - The real-time **Streamlit Dashboard** queries the PostgreSQL database (public schema) directly to display dynamic metrics, Salary Trends, Top Skills, and Top Companies with premium visualization.

---

## 📂 Project Structure & Modules

- **`airflow/dags/`**: Contains the orchestration pipelines.
  - `dag_crawl_and_ingest.py`: Scheduled DAG for scraping and pushing to Kafka. Auto-triggers DAG 2.
  - `dag_transform_and_model.py`: Event-driven DAG executing PySpark ETL $\rightarrow$ dbt run $\rightarrow$ dbt test.
- **`crawlers/`**: Crawler scripts. `itviec_crawler.py` handles dynamic extraction via Playwright. `topcv_crawler.py` (placeholder) for TopCV.
- **`dashboard/`**: Streamlit web application (`app.py`) and its custom Dockerfile.
- **`data/`**: Backup directory storing the pure JSON dumps emitted by the crawler.
- **`dbt/`**: The dbt project. Contains `models/` (staging and marts) and `schema.yml` for data quality testing.
- **`infra/`**: Infrastructure initialization scripts for Kafka (auto-create topics) and PostgreSQL (create `job_market` and `airflow` DBs).
- **`kafka_app/`**: Contains `kafka_producer.py` (invoked by crawler) and `kafka_consumer.py` (persistent daemon).
- **`spark_jobs/`**: PySpark logic for data cleaning, text parsing, and role standardization (`process_analytics.py`).
- **`utils/`**: Helper scripts, notably `launch_chrome_cdp.ps1` to open the host Chrome browser with the CDP port exposed.
- **`docker-compose.yml`**: Docker orchestration configuring all services (PostgreSQL, Zookeeper, Kafka, Kafka-UI, Consumer, Airflow, Dashboard).
- **`Makefile`**: Centralized script to run common project administrative commands.

---

## 🚀 Getting Started

### 1. Prerequisites
- **Git**
- **Docker** and **Docker Compose**
- **Google Chrome** installed on your Windows Host (Required for CDP crawling to bypass Cloudflare).

### 2. Installation
Clone the repository and set up environment variables:

```bash
git clone https://github.com/your-repo/job-market-pipeline.git
cd job-market-pipeline
cp .env.example .env
```

### 3. Build & Run the Pipeline
We use `make` commands to simplify operations.

**Step A (First time only): Build all custom Docker images**
```bash
make build-all
```
*(This will build `jobmarket-airflow`, `jobmarket-consumer`, and `jobmarket-dashboard` images using `python:3.9` dependencies).*

**Step B: Start the system**
```bash
make up
```
*(This command automatically opens a Chrome browser in debugging mode via PowerShell, then starts all Docker containers in detached mode).*

### 4. Important URLs to Monitor

Once the containers are running and healthy, you can access the following services in your browser:

| Service | URL | Default Credentials | Description |
|---|---|---|---|
| **Airflow UI** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` | Trigger and monitor data pipelines (DAGs). |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | *None* | View interactive Job Market insights. |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | *None* | Inspect Kafka topics, brokers, and raw messages. |

### 5. Running the Full Pipeline

1. Open the **Airflow UI** ([http://localhost:8081](http://localhost:8081)).
2. Enable both DAGs (`dag_crawl_and_ingest` and `dag_transform_and_model`).
3. Click **Trigger DAG** on `dag_crawl_and_ingest`.
4. Wait for it to complete. It will automatically trigger `dag_transform_and_model`.
5. Check the logs in Airflow or use `make logs-consumer` to see data flowing into PostgreSQL.
6. Open the **Streamlit Dashboard** ([http://localhost:8501](http://localhost:8501)) to see the real-time data visualized.

---

## 🛠️ Makefile Command Reference

- `make up`: Start all services and launch Chrome CDP.
- `make down`: Stop all services gracefully.
- `make restart`: Restart the pipeline.
- `make build-all`: Rebuild custom Docker images with `--no-cache`.
- `make logs-airflow`: View Airflow Webserver logs in real-time.
- `make logs-dashboard`: View Dashboard logs in real-time.
- `make logs-consumer`: View Kafka Consumer logs in real-time.
- `make clean`: Wipe out all containers and **ALL data volumes** (PostgreSQL, Kafka). Use with caution!

---

⚠️ *The project is still in development and may have some issues.* ⚠️
