# Environment verification — UPDATED 

## Purpose
- Quick checks and fixes to verify the local Docker big‑data stack (HDFS, Hive Metastore, Spark, HBase).
- Notes on common Windows/Git Bash problems and the correct Spark paths used in this project.

## 1) Check running containers (architecture)
- See which services run and their names:
```bash
docker ps -a
```
- Look for these containers (names used in this repo):
  - `namenode`, `datanode`
  - `hive-metastore-postgresql` (Postgres), `hive-metastore`
  - `spark-master` (in your environment the instance is named `spark-master-new`)
  - `spark-worker`
  - `hbase-master`, `hbase-regionserver`
  - `zookeeper`

Note: container names may differ. Always use the actual name shown by `docker ps -a` (e.g. `spark-master-new`).

## 2) Common Windows / Git Bash issues and fixes

### a) CRLF vs LF (script failures)
- Symptom: logs show `/bin/bash^M: bad interpreter` or `$'\r': command not found`.
- Cause: shell scripts have Windows line endings (CRLF). Containers need LF.
- Fix (convert files to Unix EOL on host):
  - Using `dos2unix`:
    ```bash
    dos2unix bigdata_project/initdb/hdfs-init.sh
    dos2unix bigdata_project/hive-startup.sh
    dos2unix bigdata_project/hiveserver-startup.sh
    ```
  - Prevent Git from reintroducing CRLF:
    ```bash
    git config --global core.autocrlf false
    ```

### b) Git Bash path conversion (MSYS) and TTY problems
- Problem A: Git Bash rewrites `/opt/...` into Windows path -> `docker exec` fails.
  - Workaround: disable MSYS path conversion for the single command:
    ```bash
    MSYS_NO_PATHCONV=1 docker exec -i <container> <command>
    ```
- Problem B: "the input device is not a TTY" when using heredoc or interactive TTY.
  - For interactive `docker exec` in Git Bash use `winpty`:
    ```bash
    MSYS_NO_PATHCONV=1 winpty docker exec -it <container> bash
    ```
  - When feeding heredoc / non-interactive input use `-i` only (no `-t`).

## 3) Correct Spark paths (project specifics)
- In these images Spark binaries live at `/spark/bin/` and examples at `/spark/examples/jars/`.
- Wrong path often used in docs: `/opt/spark/...` — may not exist in the container image you run.
- Find `spark-submit` / `spark-shell` inside the container:
```bash
MSYS_NO_PATHCONV=1 docker exec -i spark-master-new bash -lc 'command -v spark-submit || find / -name "spark-submit" 2>/dev/null | head -n 20'
MSYS_NO_PATHCONV=1 docker exec -i spark-master-new bash -lc 'command -v spark-shell || find / -name "spark-shell" 2>/dev/null | head -n 20'
```
- Example correct commands (use actual container name from `docker ps`):
  - Non‑interactive `spark-submit`:
    ```bash
    MSYS_NO_PATHCONV=1 docker exec -i spark-master-new /spark/bin/spark-submit \
      --master spark://spark-master-new:7077 \
      --class org.apache.spark.examples.SparkPi \
      /spark/examples/jars/spark-examples_2.12-3.1.1.jar 10
    ```
  - Non‑interactive `spark-shell` + one SQL command:
    ```bash
    MSYS_NO_PATHCONV=1 docker exec -i spark-master-new /spark/bin/spark-shell --master spark://spark-master-new:7077 --conf spark.sql.catalogImplementation=hive -i /dev/stdin <<'EOF'
    spark.sql("SHOW DATABASES").show()
    EOF
    ```

## 4) Verify Hive Metastore and HDFS init
- Hive Metastore logs:
```bash
docker logs hive-metastore --tail 200
```
- HDFS init logs:
```bash
docker logs hdfs-init --tail 200
```

## 5) HBase quick checks
- Non‑interactive HBase shell:
```bash
MSYS_NO_PATHCONV=1 docker exec -i hbase-master hbase shell <<'EOF'
status
EOF
```

## 6) Useful general checks and troubleshooting
- Show containers and filter:
```bash
docker ps -a
docker ps --filter "name=spark"
docker ps --filter "status=exited"
```
- Inspect container image:
```bash
docker inspect spark-master-new --format '{{.Config.Image}}'
```
- Tail logs:
```bash
docker logs spark-master-new --tail 200
docker logs hive-metastore-postgresql --tail 200
```
- Recreate a failing service after fixes:
```bash
docker-compose up -d --force-recreate hive-metastore hdfs-init
# or
docker compose up -d --force-recreate hive-metastore hdfs-init
```

## 7) Summary of most common fixes (one-liners)
- Convert scripts to LF and restart:
```bash
sed -i 's/\r$//' bigdata_project/initdb/*.sh bigdata_project/*.sh && docker-compose up -d --force-recreate hive-metastore hdfs-init
```
- Run Spark submit using the correct container name and binary path:
```bash
MSYS_NO_PATHCONV=1 docker exec -i spark-master-new /spark/bin/spark-submit --master spark://spark-master-new:7077 /spark/examples/jars/spark-examples_2.12-3.1.1.jar 10
```

## Notes
- Always use the container name shown by `docker ps -a`.
- Convert EOL once on host, then restart affected containers.
- Use `MSYS_NO_PATHCONV` and `winpty` in Git Bash to avoid path/TTY issues.






-----------------Old-saif-version  if work okk :) -------------------------------------------------------






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