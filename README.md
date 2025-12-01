# Big Data Project

## Environment Setup

This project uses a Docker container to host the Big Data stack (Hadoop, Hive, HBase, Spark).

### Prerequisites
- Docker Desktop installed and running.

### Quick Start

1.  **Build and Start**:
    ```bash
    docker-compose up -d --build
    ```
    This may take a while (downloading Hadoop/Spark/etc.).

2.  **Verify Services**:
    - HDFS: [http://localhost:9870](http://localhost:9870)
    - YARN: [http://localhost:8088](http://localhost:8088)
    - HBase: [http://localhost:16010](http://localhost:16010)
    - Spark: [http://localhost:8080](http://localhost:8080)

3.  **Run Verification Scripts**:
    ```bash
    # Test Hive Integration
    docker exec -it bigdata-stack /opt/spark/bin/spark-submit /scripts/test_hive.py

    # Test HBase Integration
    docker exec -it bigdata-stack bash /scripts/test_hbase.sh
    ```

4.  **Access Shell**:
    ```bash
    docker exec -it bigdata-stack bash
    ```

### Components
- **Hadoop 3.3.6** (Pseudo-distributed)
- **Hive 3.1.3** (Derby Metastore)
- **HBase 2.5.5** (Distributed on HDFS)
- **Spark 3.5.0** (YARN mode)

## Project Structure
- `docker/`: Dockerfile and configs.
- `data/`: Shared data directory (mounted to `/data_host` in container).
- `scripts/`: User scripts.
