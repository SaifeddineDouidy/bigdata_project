#!/bin/bash

# Start SSH (needed for Hadoop)
service ssh start

# Wait for SSH to be ready
echo "Waiting for SSH to be ready..."
for i in {1..30}; do
    if ssh -o ConnectTimeout=1 -o StrictHostKeyChecking=no localhost exit 2>/dev/null; then
        echo "SSH is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "WARNING: SSH may not be fully ready"
    fi
    sleep 1
done

# Format NameNode if not already formatted
if [ ! -d "/data/hdfs/namenode/current" ]; then
    echo "Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format
fi


# Start Hadoop (HDFS + YARN)
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh
echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# Create Hive directories in HDFS
echo "Creating Hive directories in HDFS..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /tmp
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs

# Initialize Hive Metastore Schema (Derby)
# We use a local directory for Derby to persist it if volume is mounted, or just inside container
if [ ! -d "metastore_db" ]; then
    echo "Initializing Hive Metastore Schema..."
    $HIVE_HOME/bin/schematool -dbType derby -initSchema
fi

# Start Hive Metastore and HiveServer2
echo "Starting Hive Metastore..."
nohup $HIVE_HOME/bin/hive --service metastore > /var/log/hive-metastore.log 2>&1 &
echo "Starting HiveServer2..."
nohup $HIVE_HOME/bin/hive --service hiveserver2 > /var/log/hiveserver2.log 2>&1 &

# Start HBase
echo "Starting HBase..."
$HBASE_HOME/bin/start-hbase.sh
echo "Starting HBase Thrift..."
$HBASE_HOME/bin/hbase-daemon.sh start thrift
echo "Starting HBase REST..."
$HBASE_HOME/bin/hbase-daemon.sh start rest

echo "Big Data Stack Started."
echo "Hadoop: 9870, 8088"
echo "Hive: 10000"
echo "HBase: 16010, 9090, 8085"
echo "Spark: 8080"

# Keep container running
tail -f /dev/null
