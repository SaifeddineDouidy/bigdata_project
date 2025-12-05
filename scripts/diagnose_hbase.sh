#!/bin/bash
# Diagnostic script for HBase initialization issues

set -e

echo "=========================================="
echo "HBASE DIAGNOSTIC SCRIPT"
echo "=========================================="
echo ""

# 1. Check ZooKeeper connectivity
echo "1. Checking ZooKeeper connectivity..."
if docker exec zookeeper echo ruok | nc 127.0.0.1 2181 | grep -q imok; then
    echo "✓ ZooKeeper is running"
else
    echo "✗ ZooKeeper is not responding"
fi

# 2. Check if /hbase znode exists in ZooKeeper
echo ""
echo "2. Checking for /hbase znode in ZooKeeper..."
ZK_LS=$(docker exec zookeeper zkCli.sh -server localhost:2181 ls / 2>&1 | grep -E "\[|hbase" || echo "")
if echo "$ZK_LS" | grep -q hbase; then
    echo "✓ /hbase znode exists"
    docker exec zookeeper zkCli.sh -server localhost:2181 ls /hbase 2>&1 | head -20
else
    echo "✗ /hbase znode does NOT exist - HBase has not initialized"
fi

# 3. Check HDFS connectivity from HBase container
echo ""
echo "3. Checking HDFS connectivity from HBase Master container..."
HDFS_TEST=$(docker exec hbase-master bash -c "hadoop fs -ls hdfs://namenode:8020/" 2>&1 || echo "FAILED")
if echo "$HDFS_TEST" | grep -q "Found"; then
    echo "✓ HBase can connect to HDFS"
    echo "  HDFS root contents:"
    echo "$HDFS_TEST" | head -5
else
    echo "✗ HBase cannot connect to HDFS"
    echo "  Error: $HDFS_TEST"
fi

# 4. Check if HBase can write to HDFS /hbase directory
echo ""
echo "4. Checking HDFS /hbase directory..."
HDFS_HBASE=$(docker exec namenode hdfs dfs -ls /hbase 2>&1 || echo "NOT_EXISTS")
if echo "$HDFS_HBASE" | grep -q "No such file"; then
    echo "⚠ /hbase directory does not exist in HDFS (will be created by HBase)"
    echo "  Attempting to create /hbase directory with proper permissions..."
    docker exec namenode hdfs dfs -mkdir -p /hbase 2>&1 || true
    docker exec namenode hdfs dfs -chmod 777 /hbase 2>&1 || true
    echo "  Created /hbase directory"
else
    echo "✓ /hbase directory exists in HDFS"
    echo "$HDFS_HBASE" | head -5
fi

# 5. Check HBase Master process
echo ""
echo "5. Checking HBase Master process..."
HM_PROCESS=$(docker exec hbase-master bash -c "ps aux | grep -E 'HMaster' | grep -v grep" 2>&1 || echo "")
if [ -n "$HM_PROCESS" ]; then
    echo "✓ HMaster process is running"
    echo "  PID: $(echo "$HM_PROCESS" | awk '{print $2}')"
else
    echo "✗ HMaster process is NOT running"
fi

# 6. Check HBase Master logs for errors
echo ""
echo "6. Recent HBase Master log errors (last 30 lines):"
docker logs hbase-master --tail 30 2>&1 | grep -iE "error|exception|failed|fatal" | tail -10 || echo "No errors found in recent logs"

# 7. Check HBase configuration files in container
echo ""
echo "7. Checking HBase configuration files in container..."
echo "  hbase-site.xml exists:"
docker exec hbase-master bash -c "test -f /opt/hbase/conf/hbase-site.xml && echo '    ✓ /opt/hbase/conf/hbase-site.xml'" || echo "    ✗ /opt/hbase/conf/hbase-site.xml not found"
docker exec hbase-master bash -c "test -f /hbase/conf/hbase-site.xml && echo '    ✓ /hbase/conf/hbase-site.xml'" || echo "    ✗ /hbase/conf/hbase-site.xml not found"

echo ""
echo "  core-site.xml exists:"
docker exec hbase-master bash -c "test -f /opt/hbase/conf/core-site.xml && echo '    ✓ /opt/hbase/conf/core-site.xml'" || echo "    ✗ /opt/hbase/conf/core-site.xml not found"

echo ""
echo "  hbase-env.sh exists:"
docker exec hbase-master bash -c "test -f /opt/hbase/conf/hbase-env.sh && echo '    ✓ /opt/hbase/conf/hbase-env.sh'" || echo "    ✗ /opt/hbase/conf/hbase-env.sh not found"

# 8. Check ZooKeeper connection from HBase
echo ""
echo "8. Testing ZooKeeper connection from HBase Master..."
ZK_CONN_TEST=$(docker exec hbase-master bash -c "echo ruok | nc zookeeper 2181" 2>&1 || echo "FAILED")
if echo "$ZK_CONN_TEST" | grep -q imok; then
    echo "✓ HBase can connect to ZooKeeper"
else
    echo "✗ HBase cannot connect to ZooKeeper"
    echo "  Response: $ZK_CONN_TEST"
fi

# 9. Check HBase Master web UI
echo ""
echo "9. Checking HBase Master web UI..."
UI_TEST=$(curl -s http://localhost:16010 2>&1 | head -5 || echo "FAILED")
if echo "$UI_TEST" | grep -q "HBase"; then
    echo "✓ HBase Master web UI is accessible"
else
    echo "✗ HBase Master web UI is not accessible"
fi

echo ""
echo "=========================================="
echo "DIAGNOSTIC COMPLETE"
echo "=========================================="

