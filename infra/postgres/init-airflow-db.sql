-- Create additional databases on first PostgreSQL startup.
-- This script runs automatically via docker-entrypoint-initdb.d.

-- Airflow metadata database
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
