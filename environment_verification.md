
# Environment Verification Guide

This document provides a step-by-step guide to verify that your Big Data environment (Hadoop, Hive, HBase, Spark) is correctly set up and ready for the project tasks.

## 1. Verify Docker Containers

Ensure all services are up and running.

```bash
docker-compose ps
```

**Expected Output:** All services (`namenode`, `datanode`, `hive-server`, `hive-metastore`, `spark-master`, `spark-worker`, `hbase-master`, `hbase-regionserver`, `zookeeper`) should be in the `Up` (healthy) state.

## 2. Verify HDFS (Hadoop Distributed File System)

Check if HDFS is accessible and writable.

```bash
# List root directory
docker exec -it namenode hdfs dfs -ls /

# Create a test directory
docker exec -it namenode hdfs dfs -mkdir -p /user/test

# Verify directory creation
docker exec -it namenode hdfs dfs -ls /user
```

## 3. Verify Hive

Check if Hive Metastore is running and accessible from Spark.

Since HiveServer2 is not configured in this setup, Hive operations will be performed via Spark SQL with Hive support enabled.

To verify, you can use Spark to connect to the metastore:

```bash
docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.sql.catalogImplementation=hive -i /dev/stdin <<EOF
spark.sql("SHOW DATABASES").show()
EOF
```

**Expected Output:** Should list `default` and any other existing databases.

## 4. Verify HBase

Check if HBase is running and accessible.

```bash
# Check HBase status
docker exec -it hbase-master echo "status" | hbase shell
```

**Expected Output:** Should show the number of servers (1 master, 1 regionserver).

## 5. Verify Spark

Check if Spark is running and can communicate with Hive.

### 5.1 Basic Spark Shell
```bash
docker exec -it spark-master /opt/spark/bin/spark-shell
```
*Type `:quit` to exit.*

### 5.2 Verify Spark-Hive Integration
Run a simple PySpark script to check if Spark can see Hive tables.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar 10
```

## 6. Verify Avro Support (Crucial for Project)

Since your project involves Avro, we need to ensure Spark can handle it. You will likely need to include the `spark-avro` package when submitting jobs.

Test it by launching a Spark shell with the Avro package:

```bash
docker exec -it spark-master /opt/spark/bin/spark-shell --packages org.apache.spark:spark-avro_2.12:3.1.1
```

Inside the shell, try importing the package:
```scala
import org.apache.spark.sql.avro._
println("Avro package loaded successfully")