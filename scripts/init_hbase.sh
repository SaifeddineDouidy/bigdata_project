#!/bin/bash
# HBase initialization script to ensure proper startup
# This script can be run inside the HBase Master container to verify and fix configuration

set -e

echo "HBase Initialization Script"
echo "============================"

# Wait for ZooKeeper to be ready
echo "Waiting for ZooKeeper..."
until echo ruok | nc zookeeper 2181 | grep -q imok; do
    echo "ZooKeeper not ready, waiting..."
    sleep 2
done
echo "✓ ZooKeeper is ready"

# Wait for HDFS to be ready
echo "Waiting for HDFS..."
until hadoop fs -ls hdfs://namenode:8020/ >/dev/null 2>&1; do
    echo "HDFS not ready, waiting..."
    sleep 2
done
echo "✓ HDFS is ready"

# Ensure /hbase directory exists in HDFS with proper permissions
echo "Checking HDFS /hbase directory..."
if ! hadoop fs -test -d hdfs://namenode:8020/hbase 2>/dev/null; then
    echo "Creating /hbase directory in HDFS..."
    hadoop fs -mkdir -p hdfs://namenode:8020/hbase
    hadoop fs -chmod 777 hdfs://namenode:8020/hbase
    echo "✓ Created /hbase directory"
else
    echo "✓ /hbase directory exists"
    hadoop fs -chmod 777 hdfs://namenode:8020/hbase || true
fi

# Verify configuration files
echo "Verifying configuration files..."
if [ -f /opt/hbase/conf/hbase-site.xml ]; then
    echo "✓ hbase-site.xml found at /opt/hbase/conf/hbase-site.xml"
    echo "  hbase.rootdir: $(grep -A1 'hbase.rootdir' /opt/hbase/conf/hbase-site.xml | grep value | sed 's/.*<value>\(.*\)<\/value>.*/\1/')"
    echo "  hbase.zookeeper.quorum: $(grep -A1 'hbase.zookeeper.quorum' /opt/hbase/conf/hbase-site.xml | grep value | sed 's/.*<value>\(.*\)<\/value>.*/\1/')"
elif [ -f /hbase/conf/hbase-site.xml ]; then
    echo "✓ hbase-site.xml found at /hbase/conf/hbase-site.xml"
else
    echo "✗ hbase-site.xml not found!"
fi

echo "============================"
echo "Initialization complete"

