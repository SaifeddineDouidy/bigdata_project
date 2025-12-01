#!/bin/bash
# Complete HBase reset and reinitialization

echo "Step 1: Stopping all HBase services..."
/opt/hbase/bin/stop-hbase.sh
sleep 5

# Force kill if still running
pkill -9 -f HMaster
pkill -9 -f HRegionServer  
pkill -9 -f HQuorumPeer
sleep 2

echo "Step 2: Cleaning HBase data from HDFS..."
hdfs dfs -rm -r /hbase/* 2>/dev/null || true
sleep 2

echo "Step 3: Cleaning local ZooKeeper data..."
rm -rf /opt/hbase/zookeeper/*
sleep 2

echo "Step 4: Starting HBase fresh..."
/opt/hbase/bin/start-hbase.sh

echo "Step 5: Waiting for HBase to initialize (90 seconds)..."
sleep 90

echo "Step 6: Checking HBase status..."
/opt/hbase/bin/hbase shell <<EOF
status
list
exit
EOF

echo "HBase reset complete!"
