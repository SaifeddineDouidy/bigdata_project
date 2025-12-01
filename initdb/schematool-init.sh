#!/bin/bash
set -euo pipefail

# Configurable via env or defaults
DB_HOST="${DB_HOST:-hive-metastore-postgresql}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-hiveuser}"
DB_NAME="${DB_NAME:-metastore}"
RETRY_INTERVAL=5
MAX_RETRIES=60

# Wait for Postgres to be ready
tries=0
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" >/dev/null 2>&1; do
  tries=$((tries+1))
  if [ "$tries" -ge "$MAX_RETRIES" ]; then
    echo "Postgres did not become ready after $MAX_RETRIES tries."
    exit 1
  fi
  echo "Waiting for Postgres... ($tries/$MAX_RETRIES)"
  sleep $RETRY_INTERVAL
done

# Wait for HDFS namenode to be ready (use hdfs dfs -ls / as check)
tries=0
until hdfs dfs -ls / >/dev/null 2>&1; do
  tries=$((tries+1))
  if [ "$tries" -ge "$MAX_RETRIES" ]; then
    echo "HDFS not ready after $MAX_RETRIES tries."
    exit 1
  fi
  echo "Waiting for HDFS... ($tries/$MAX_RETRIES)"
  sleep $RETRY_INTERVAL
done

# Run schematool only if version info missing (schematool handles idempotency in many cases).
echo "Running schematool -dbType postgres -initSchema -verbose"
if /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "Metastore schema appears present; exiting."
  exit 0
fi

/opt/hive/bin/schematool -dbType postgres -initSchema -verbose
echo "schematool finished."
