#!/bin/bash
# =============================================================================
# Kafka Topic Initialization Script - Job Market Pipeline
# =============================================================================

set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
KAFKA_BIN="kafka-topics"

echo "[INFO] Waiting for Kafka broker to be ready at ${BOOTSTRAP_SERVER}..."

# Wait for Kafka to be fully ready
until ${KAFKA_BIN} --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
  echo "[INFO] Kafka not ready yet, retrying in 3s..."
  sleep 3
done

echo "[INFO] Kafka is ready."

# ─── Creating topic: raw_jobs ────────────────────────────────────────────────
echo "[INFO] Creating topic: raw_jobs..."
${KAFKA_BIN} \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic raw_jobs \
  --partitions 3 \
  --replication-factor 1

echo "[OK]   All topics initialized successfully."
