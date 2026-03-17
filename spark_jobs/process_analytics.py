import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, concat_ws
from pyspark.sql.types import (
    StructType, StructField, StringType,
    ArrayType, IntegerType
)

# ══════════════════════════════════════════════════════════════════════════════
# DICTIONARIES & MAPPINGS
# ══════════════════════════════════════════════════════════════════════════════

# --- Skill keywords for extraction from description ---
COMMON_SKILLS = [
    # --- Backend & Core Languages ---
    "python", "java", "c++", "c#", "ruby", "php", "javascript", "typescript",
    "golang", "go", "rust", "scala", "perl", "bash", "shell",

    # --- Frontend & Mobile ---
    "react", "angular", "vue", "react native", "flutter", "ios", "android",
    "swift", "kotlin",

    # --- Web Frameworks ---
    "node.js", "nodejs", "django", "flask", "fastapi", "spring", "asp.net",
    "laravel", "express",

    # --- Databases (SQL & NoSQL) ---
    "sql", "mysql", "postgresql", "mongodb", "redis", "oracle", "nosql",
    "elasticsearch", "cassandra", "dynamodb", "neo4j", "mariadb",
    "sql server", "mssql",

    # --- Cloud & DevOps & MLOps ---
    "aws", "azure", "gcp", "docker", "kubernetes", "k8s", "terraform",
    "ci/cd", "jenkins", "github actions", "gitlab ci", "ansible", "linux",
    "mlflow", "kubeflow", "bentoml",

    # --- Data Engineering (DE) & Big Data ---
    "spark", "pyspark", "kafka", "hadoop", "airflow", "dbt", "snowflake",
    "bigquery", "redshift", "hive", "presto", "trino", "flink", "databricks",
    "etl", "elt", "data pipeline", "data warehouse", "data lake", "ssis",
    "talend", "nifi", "glue",

    # --- Data Analysis (DA) & BI ---
    "tableau", "power bi", "looker", "metabase", "superset", "excel", "vba",
    "data visualization", "google analytics", "mixpanel", "ab testing",
    "a/b testing", "statistics", "dashboard",

    # --- Data Science (DS) & Machine Learning (ML) ---
    "machine learning", "deep learning", "nlp", "tensorflow", "pytorch",
    "scikit-learn", "pandas", "numpy", "scipy", "matplotlib", "seaborn",
    "keras", "xgboost", "lightgbm", "computer vision", "opencv",
    "recommendation system", "predictive modeling", "time series",
    "data mining", "statistical modeling",

    # --- GenAI & LLMs ---
    "llm", "large language models", "langchain", "prompt engineering",
    "openai", "huggingface", "stable diffusion", "midjourney", "rag",
    "vector database", "pinecone", "milvus", "qdrant",
]

# --- Role standardization mapping ---
# Each tuple: (keyword_to_search, standardized_role)
# Order matters: more specific roles checked FIRST
ROLE_MAPPING = [
    # Data roles
    ("data engineer", "Data Engineer"),
    ("data analyst", "Data Analyst"),
    ("data scientist", "Data Scientist"),
    ("data architect", "Data Architect"),
    ("analytics engineer", "Analytics Engineer"),
    ("bi developer", "BI Developer"),
    ("bi analyst", "BI Developer"),
    ("business intelligence", "BI Developer"),
    ("etl developer", "Data Engineer"),
    ("big data", "Data Engineer"),

    # AI / ML roles
    ("machine learning", "ML Engineer"),
    ("ml engineer", "ML Engineer"),
    ("ai engineer", "AI Engineer"),
    ("artificial intelligence", "AI Engineer"),
    ("nlp engineer", "NLP Engineer"),
    ("deep learning", "ML Engineer"),
    ("computer vision", "ML Engineer"),

    # Backend roles
    ("backend", "Backend Developer"),
    ("back-end", "Backend Developer"),
    ("back end", "Backend Developer"),

    # Frontend roles
    ("frontend", "Frontend Developer"),
    ("front-end", "Frontend Developer"),
    ("front end", "Frontend Developer"),

    # Fullstack roles
    ("fullstack", "Fullstack Developer"),
    ("full-stack", "Fullstack Developer"),
    ("full stack", "Fullstack Developer"),

    # Mobile roles
    ("mobile", "Mobile Developer"),
    ("ios developer", "iOS Developer"),
    ("android developer", "Android Developer"),
    ("flutter", "Mobile Developer"),
    ("react native", "Mobile Developer"),

    # DevOps / Cloud
    ("devops", "DevOps Engineer"),
    ("sre", "DevOps Engineer"),
    ("site reliability", "DevOps Engineer"),
    ("cloud engineer", "Cloud Engineer"),
    ("infrastructure", "Cloud Engineer"),
    ("platform engineer", "Cloud Engineer"),

    # QA / Testing
    ("qa", "QA Engineer"),
    ("quality assurance", "QA Engineer"),
    ("tester", "QA Engineer"),
    ("test engineer", "QA Engineer"),
    ("automation test", "QA Engineer"),

    # Security
    ("security", "Security Engineer"),
    ("cybersecurity", "Security Engineer"),
    ("pentest", "Security Engineer"),

    # Project / Product
    ("project manager", "Project Manager"),
    ("product manager", "Product Manager"),
    ("scrum master", "Scrum Master"),
    ("product owner", "Product Owner"),
    ("business analyst", "Business Analyst"),

    # Design
    ("ui/ux", "UI/UX Designer"),
    ("ux designer", "UI/UX Designer"),
    ("ui designer", "UI/UX Designer"),

    # Catch-all dev
    ("software engineer", "Software Engineer"),
    ("developer", "Software Engineer"),
    ("programmer", "Software Engineer"),
    ("engineer", "Software Engineer"),
]

# --- Experience level standardization ---
# Order matters: more specific levels checked FIRST
LEVEL_MAPPING = [
    # Intern
    ("intern", "Intern"),
    ("thực tập", "Intern"),
    ("internship", "Intern"),

    # Fresher
    ("fresher", "Fresher"),
    ("fresh graduate", "Fresher"),
    ("entry level", "Fresher"),
    ("entry-level", "Fresher"),

    # Junior
    ("junior", "Junior"),

    # Middle / Mid / Intermediate
    ("middle", "Middle"),
    ("mid-level", "Middle"),
    ("mid level", "Middle"),
    ("intermediate", "Middle"),

    # Senior
    ("senior", "Senior"),
    ("sr.", "Senior"),
    ("sr ", "Senior"),

    # Lead / Principal
    ("lead", "Lead"),
    ("principal", "Lead"),
    ("staff", "Lead"),
    ("team lead", "Lead"),
    ("tech lead", "Lead"),

    # Manager / Head / Director
    ("manager", "Manager"),
    ("head of", "Manager"),
    ("director", "Manager"),
    ("vp ", "Manager"),
    ("chief", "Manager"),
]


# ══════════════════════════════════════════════════════════════════════════════
# UDF FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def parse_salary_string(salary_str):
    """
    Parse raw salary text into structured min, max, currency.

    Examples:
        '1,000 - 2,000 USD' -> min=1000, max=2000, currency=USD
        'Lên đến 30,000,000 VNĐ' -> min=None, max=30000000, currency=VND
        'You'll love it' -> min=None, max=None, currency=Unknown
    """
    if not salary_str or salary_str.lower() in [
        "thương lượng", "you'll love it",
        "login to view salary", "sign in to view salary",
    ]:
        return {"min_salary": None, "max_salary": None, "currency": "Unknown"}

    salary_str = salary_str.upper().replace(",", "")

    currency = "USD"
    if "VND" in salary_str or "VNĐ" in salary_str:
        currency = "VND"
    elif "USD" not in salary_str:
        currency = "Unknown"

    numbers = [int(n) for n in re.findall(r"\d+", salary_str)]

    min_sal, max_sal = None, None
    if len(numbers) >= 2:
        min_sal, max_sal = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if any(
            kw in salary_str
            for kw in ["UP TO", "LÊN ĐẾN", "TỚI", "MAXIMUM"]
        ):
            max_sal = numbers[0]
        elif any(kw in salary_str for kw in ["TỪ", "FROM", "MINIMUM"]):
            min_sal = numbers[0]
        else:
            min_sal = max_sal = numbers[0]

    return {"min_salary": min_sal, "max_salary": max_sal, "currency": currency}


def extract_unique_skills(description, explicit_skills):
    """
    Merge explicit skill tags + keyword scan on description.
    Each skill counted only ONCE per job posting (dedup via set).
    """
    skills_set = set()

    # 1. Add explicit skills from website tags
    if explicit_skills:
        for s in explicit_skills:
            if s:
                skills_set.add(s.strip().lower())

    # 2. Scan description for additional skills
    if description:
        desc_lower = description.lower()
        for skill in COMMON_SKILLS:
            pattern = r"\b" + re.escape(skill) + r"\b"
            if re.search(pattern, desc_lower):
                # Normalize aliases
                standard = skill
                if standard == "node.js":
                    standard = "nodejs"
                elif standard == "k8s":
                    standard = "kubernetes"
                skills_set.add(standard)

    return list(skills_set)


def standardize_role(title):
    """
    Map raw job title to a standardized role category.

    Examples:
        'Senior Data Engineer (Python/Spark)' -> 'Data Engineer'
        'Backend Developer - Java'             -> 'Backend Developer'
        'AI/ML Research Engineer'              -> 'AI Engineer'
    """
    if not title:
        return "Other"

    title_lower = title.lower()
    for keyword, role in ROLE_MAPPING:
        if keyword in title_lower:
            return role

    return "Other"


def standardize_level(title, expertise):
    """
    Extract experience level from title and expertise field.

    Logic:
        1. Check title first (most reliable signal).
        2. If not found, check expertise field.
        3. Default to 'Not Specified'.

    Examples:
        title='Senior Data Engineer'  -> 'Senior'
        title='Backend Developer', expertise='1-3 years' -> 'Junior'
        title='Data Analyst', expertise='Fresher' -> 'Fresher'
    """
    if not title:
        title = ""
    if not expertise:
        expertise = ""

    combined = (title + " " + expertise).lower()

    for keyword, level in LEVEL_MAPPING:
        if keyword in combined:
            return level

    # Fallback: try to guess from years of experience in expertise
    years_match = re.search(r"(\d+)\s*[-–]\s*(\d+)\s*year", combined)
    if years_match:
        min_years = int(years_match.group(1))
        if min_years == 0:
            return "Fresher"
        elif min_years <= 2:
            return "Junior"
        elif min_years <= 5:
            return "Middle"
        else:
            return "Senior"

    single_year = re.search(r"(\d+)\+?\s*year", combined)
    if single_year:
        years = int(single_year.group(1))
        if years == 0:
            return "Fresher"
        elif years <= 2:
            return "Junior"
        elif years <= 5:
            return "Middle"
        else:
            return "Senior"

    return "Not Specified"


# ══════════════════════════════════════════════════════════════════════════════
# REGISTER SPARK UDFs
# ══════════════════════════════════════════════════════════════════════════════

parse_salary_udf = udf(
    lambda s: parse_salary_string(s),
    StructType([
        StructField("min_salary", IntegerType(), True),
        StructField("max_salary", IntegerType(), True),
        StructField("currency", StringType(), True),
    ]),
)

extract_skills_udf = udf(extract_unique_skills, ArrayType(StringType()))
standardize_role_udf = udf(standardize_role, StringType())
standardize_level_udf = udf(standardize_level, StringType())


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════


def run_pipeline():
    """Run Lean ETL: raw_jobs -> analytics_jobs (single overwrite)."""

    print("Initializing PySpark Session...")
    spark = (
        SparkSession.builder
        .appName("JobMarket_Process_Analytics")
        .master("local[1]")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Database connection
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "job_market")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    conn_props = {
        "user": db_user,
        "password": db_pass,
        "driver": "org.postgresql.Driver",
    }

    # ── Read raw data ─────────────────────────────────────────────────────
    print(f"Reading from table 'raw_jobs' at {jdbc_url}...")
    try:
        raw_df = spark.read.jdbc(
            url=jdbc_url, table="raw_jobs", properties=conn_props
        )
    except Exception as e:
        print(f"Error reading from Postgres: {e}")
        return

    row_count = raw_df.count()
    if row_count == 0:
        print("No data found in 'raw_jobs'. Exiting.")
        return

    print(f"Loaded {row_count} raw records.")

    # ── Parse JSONB column ────────────────────────────────────────────────
    json_schema = StructType([
        StructField("title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("locations", StringType(), True),
        StructField("working_method", StringType(), True),
        StructField("skills", ArrayType(StringType()), True),
        StructField("expertise", StringType(), True),
        StructField("posted_date", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source", StringType(), True),
        StructField("url", StringType(), True),
        StructField("crawled_at", StringType(), True),
    ])

    parsed_df = raw_df.withColumn(
        "data", from_json(col("raw_data"), json_schema)
    )

    # ── Apply all transformations ─────────────────────────────────────────
    transformed_df = parsed_df.select(
        col("job_id"),
        col("data.title").alias("title"),
        col("data.company").alias("company"),
        col("data.locations").alias("locations"),
        col("data.working_method").alias("working_method"),
        col("data.posted_date").alias("posted_date"),
        col("data.source").alias("source"),
        col("data.url").alias("url"),

        # Salary parsing
        parse_salary_udf(col("data.salary")).alias("salary_parsed"),

        # Skills dedup & extraction
        extract_skills_udf(
            col("data.description"), col("data.skills")
        ).alias("standardized_skills"),

        # Role standardization
        standardize_role_udf(col("data.title")).alias("role"),

        # Level standardization
        standardize_level_udf(
            col("data.title"), col("data.expertise")
        ).alias("level"),
    )

    # Flatten salary struct into separate columns
    final_df = transformed_df.select(
        "job_id",
        "title",
        "role",
        "level",
        "company",
        "locations",
        "working_method",
        "salary_parsed.min_salary",
        "salary_parsed.max_salary",
        "salary_parsed.currency",
        concat_ws(",", col("standardized_skills")).alias("standardized_skills"),
        "posted_date",
        "source",
        "url",
    )

    print("Sample Transformed Data:")
    final_df.show(5, truncate=False)

    # ── Write to analytics_jobs ───────────────────────────────────────────
    print("Writing to 'analytics_jobs' table...")
    final_df.write.mode("overwrite").jdbc(
        url=jdbc_url, table="analytics_jobs", properties=conn_props
    )

    print(
        f"Done! {row_count} records processed "
        f"and written to 'analytics_jobs'."
    )
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
