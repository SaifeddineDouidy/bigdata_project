#!/bin/bash
set -e

echo "=== Starting Hive Server 2 Setup ==="

# Fix the HeapSize bug
echo "Fixing HADOOP_HEAPSIZE issue..."
if [ -f /opt/hive/bin/ext/hiveserver2.sh ]; then
    sed -i 's/export HADOOP_HEAPSIZE/# export HADOOP_HEAPSIZE/g' /opt/hive/bin/ext/hiveserver2.sh
fi

echo "Starting Hive Server 2..."
exec /opt/hive/bin/hive --service hiveserver2