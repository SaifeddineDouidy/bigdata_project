#!/bin/bash
set -uo pipefail

RETRY_INTERVAL=5
MAX_RETRIES=60
tries=0

# 1. Wait for NameNode to be safely out of Safemode and reachable
until hdfs dfs -ls / >/dev/null 2>&1; do
  tries=$((tries+1))
  if [ "$tries" -ge "$MAX_RETRIES" ]; then
    echo "HDFS not ready after $MAX_RETRIES tries."
    exit 1
  fi
  echo "Waiting for HDFS... ($tries/$MAX_RETRIES)"
  sleep $RETRY_INTERVAL
done

echo "HDFS is available. Creating directories..."

# 2. Create /hbase directory
hdfs dfs -mkdir -p /hbase

# 3. The Fix: Use chmod 777 instead of chown
# Since the 'hbase' user does not exist in this 'namenode' container,
# 'chown' will fail. 
# 777 permissions ensure the actual HBase container can write here 
# regardless of its user ID.
hdfs dfs -chmod -R 777 /hbase

# Optional: Ensure /tmp exists and is writable (Hive/Spark often need this)
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod -R 777 /tmp

echo "/hbase created and permissions set to 777."