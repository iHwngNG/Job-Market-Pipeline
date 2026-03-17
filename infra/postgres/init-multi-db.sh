#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Create multiple databases in a single PostgreSQL container.
# This script runs automatically on first container startup via
# docker-entrypoint-initdb.d.
#
# Databases created:
#   1. job_market  — main pipeline data (raw_jobs, analytics_jobs)
#   2. airflow     — Airflow metadata
# ──────────────────────────────────────────────────────────────────────────────

set -e
set -u

function create_user_and_database() {
    local database=$1
    echo "Creating database '$database'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
    echo "Database '$database' created successfully."
}

# Main DB is already created by POSTGRES_DB env var.
# Create additional databases here:
if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
    echo "Multiple databases requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
    done
    echo "All databases initialized."
fi
