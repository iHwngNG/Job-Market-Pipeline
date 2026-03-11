# Vietnamese Job Market Intelligence Pipeline
**PRD v1.0 — Fresher Data Engineer Portfolio Project**

---

## TL;DR

Crawl job listings từ TopCV & ITviec → stream qua Kafka → transform bằng Spark → model với dbt → visualize trên Streamlit. Chạy hoàn toàn local bằng Docker Compose. Ship được trong **10–14 ngày**.

---

## Mục tiêu

Showcase khả năng xây dựng **end-to-end data pipeline** ở mức production-ready:
- Ingestion: web crawling + Kafka streaming
- Processing: PySpark batch transform
- Modeling: dbt staging + mart layers
- Orchestration: Airflow DAGs
- Serving: PostgreSQL + Streamlit dashboard

Recruiter nhìn vào thấy ngay: "Người này hiểu data flow từ source đến dashboard."

---

## Architecture

```
TopCV / ITviec
     │
     ▼
[Scrapy Crawler]  ──►  [Kafka Topic: raw_jobs]
                              │
                              ▼
                    [PySpark Batch Job]
                    - Dedup, clean, normalize
                              │
                              ▼
                    [PostgreSQL: bronze layer]
                              │
                              ▼
                         [dbt]
                    staging → mart
                              │
                              ▼
                    [Streamlit Dashboard]

Orchestration: Airflow (2 DAGs)
Infrastructure: Docker Compose
```

---

## Tech Stack

| Layer | Tool | Lý do chọn |
|---|---|---|
| Crawling | Scrapy + Playwright | Handle JS-rendered pages |
| Streaming | Kafka (Confluent image) | Industry standard |
| Processing | PySpark | Core DE skill |
| Modeling | dbt-core + dbt-postgres | Recruiter cực kỳ quan tâm |
| Orchestration | Airflow 2.x | Standard workflow tool |
| Storage | PostgreSQL | Đơn giản, đủ dùng |
| Dashboard | Streamlit | Nhanh, đẹp, Python native |
| Infra | Docker Compose | Reproduce được ở mọi máy |

---

## Scope — 4 Modules (cắt từ 9 xuống 4)

### Module 1: Data Ingestion (Crawl + Kafka)
**Deliverables:**
- Scrapy spider cho TopCV: crawl job title, company, salary, location, skills, posted_date
- Scrapy spider cho ITviec: tương tự
- Kafka producer: push raw JSON vào topic `raw_jobs`
- Kafka consumer: consume và lưu vào PostgreSQL bronze table

**Scope giới hạn:**
- Crawl 500–1000 jobs/lần (không cần pagination vô hạn)
- Không cần real-time streaming, chạy batch mỗi ngày là đủ
- Không handle anti-bot phức tạp (dùng delays + user-agent rotation cơ bản)

---

### Module 2: Data Processing (PySpark)
**Deliverables:**
- PySpark job: đọc từ PostgreSQL bronze → clean → write silver
- Transformations:
  - Normalize salary (convert ranges → min/max numeric)
  - Extract skills từ free-text description (keyword matching)
  - Standardize location (Hà Nội, HCM, Remote...)
  - Dedup theo (company + title + posted_date)

**Scope giới hạn:**
- Không cần Spark cluster, chạy local mode là đủ
- Không cần Structured Streaming (batch đủ ấn tượng nếu dbt tốt)

---

### Module 3: Data Modeling (dbt)
**Deliverables:**
- `staging/stg_jobs.sql` — clean view từ silver table
- `marts/dim_skills.sql` — skill frequency theo source/time
- `marts/dim_companies.sql` — top hiring companies
- `marts/fct_salary_trends.sql` — salary range theo role/location
- `marts/fct_market_overview.sql` — tổng hợp cho dashboard
- `schema.yml` với tests: not_null, unique, accepted_values
- `sources.yml` documented đầy đủ

**Scope giới hạn:**
- 4–5 models là đủ, không cần nhiều hơn
- Focus vào data quality tests, recruiter rất chú ý điểm này

---

### Module 4: Orchestration + Dashboard
**Deliverables:**

**Airflow — 2 DAGs:**
- `dag_crawl_and_ingest`: Scrapy → Kafka → Bronze (chạy daily 8am)
- `dag_transform_and_model`: PySpark → Silver → dbt run (chạy sau DAG 1)

**Streamlit Dashboard — 4 views:**
1. Market Overview: tổng jobs, top locations, top sources
2. Top Skills: bar chart skills phổ biến nhất theo role
3. Salary Insights: salary distribution theo role/location
4. Company Rankings: companies đang tuyển nhiều nhất

---

## Repo Structure

```
vn-job-market-pipeline/
├── docker-compose.yml
├── .env.example
├── README.md                    # ← Đây là thứ recruiter đọc đầu tiên
│
├── crawlers/
│   ├── topcv_spider.py
│   ├── itviec_spider.py
│   └── kafka_producer.py
│
├── spark_jobs/
│   └── transform_bronze_to_silver.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│       ├── dag_crawl_and_ingest.py
│       └── dag_transform_and_model.py
│
└── dashboard/
    └── app.py
```

---

## Timeline — 12 ngày

| Ngày | Việc cần làm |
|---|---|
| 1 | Setup Docker Compose: Kafka, PostgreSQL, Airflow, Spark |
| 2–3 | Viết Scrapy spiders (TopCV + ITviec), test crawl |
| 4 | Kafka producer/consumer, bronze table schema |
| 5–6 | PySpark transformation job (bronze → silver) |
| 7–8 | dbt models + tests (staging + 4 marts) |
| 9 | Airflow DAGs kết nối toàn bộ pipeline |
| 10 | Streamlit dashboard (4 views) |
| 11 | End-to-end test, fix bugs |
| 12 | README hoàn chỉnh + demo recording |

---

## README — Phần quan trọng nhất với Recruiter

README cần có:
1. **Architecture diagram** (dùng Mermaid hoặc draw.io)
2. **Quick start**: `docker-compose up` là chạy được
3. **What this pipeline does** — giải thích bằng tiếng Anh, rõ ràng
4. **Key engineering decisions** — tại sao chọn Kafka thay vì direct insert, tại sao dbt, v.v.
5. **Sample dashboard screenshot**
6. **Data lineage** — từ source đến mart

---

## Điểm ấn tượng với Recruiter (so với Fresher thông thường)

| Fresher thông thường | Project này |
|---|---|
| Dùng CSV/static data | Data thật từ job market VN |
| Không có orchestration | Airflow DAGs với dependencies |
| Không có data quality | dbt tests: not_null, unique, custom |
| Script rời rạc | End-to-end pipeline reproducible |
| Không có documentation | README như senior engineer |
| Không có data modeling | dbt marts với business logic rõ ràng |

---

## Những thứ KHÔNG làm (để ship đúng hạn)

- ❌ Real-time Structured Streaming (batch là đủ)
- ❌ Cloud deployment (Docker local là đủ)
- ❌ ML/AI features
- ❌ Authentication cho dashboard
- ❌ Crawl toàn bộ site (500–1000 jobs là đủ để demo)
- ❌ CI/CD pipeline

---

*"Done is better than perfect. Ship it, then iterate."*
