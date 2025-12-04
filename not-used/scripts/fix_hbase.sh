#!/bin/bash
# Fix HBase initialization issues

echo "Stopping HBase services..."
/opt/hbase/bin/stop-hbase.sh

# Wait for processes to fully stop
sleep 10

# Remove the problematic initializing flag if it exists
echo "Cleaning up HBase metadata..."
hdfs dfs -rm -f /hbase/MasterData/data/master/store/.initializing 2>/dev/null

# Start HBase
echo "Starting HBase..."
/opt/hbase/bin/start-hbase.sh

# Wait for HBase to initialize
echo "Waiting for HBase to initialize (60 seconds)..."
sleep 60

echo "HBase restart complete. Try your test again."
