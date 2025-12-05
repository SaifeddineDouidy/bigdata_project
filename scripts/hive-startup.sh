#!/bin/bash
set -e

echo "=== Starting Hive Metastore Setup ==="

# 1. Fix the HeapSize bug
echo "Fixing HADOOP_HEAPSIZE issue..."
sed -i 's/export HADOOP_HEAPSIZE/# export HADOOP_HEAPSIZE/g' /opt/hive/bin/ext/metastore.sh

# 2. Wait for PostgreSQL to be ready (using netcat or sleep)
echo "Waiting for PostgreSQL..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  if nc -z hive-metastore-postgresql 5432 2>/dev/null; then
    echo "PostgreSQL is ready!"
    break
  fi
  echo "PostgreSQL is unavailable - sleeping (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"
  sleep 2
  RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "ERROR: PostgreSQL did not become ready in time"
  exit 1
fi

# 3. Additional wait to ensure PostgreSQL is fully initialized
sleep 5

# 4. Initialize schema using schematool (Hive 2.3.2 syntax)
echo "Initializing Hive Metastore Schema..."
/opt/hive/bin/schematool -dbType postgres -initSchema || {
    echo "Schema initialization failed or already exists. Checking schema info..."
    /opt/hive/bin/schematool -dbType postgres -info || true
}

# 5. Start the Metastore service
echo "Starting Hive Metastore Service..."
exec /opt/hive/bin/hive --service metastore