# Big Data Project

## Environment Setup

This project uses Docker Compose to orchestrate a multi-service Big Data stack including Hadoop, Hive, HBase, Spark, and Zookeeper.

### Prerequisites
- Docker Desktop installed and running.

### Quick Start

1.  **Start Services**:
    ```bash
    docker-compose up -d
    ```
    This may take a while (downloading images).

2.  **Verify Services**:
    - Namenode (HDFS): [http://localhost:9870](http://localhost:9870)
    - Datanode: [http://localhost:9864](http://localhost:9864)
    - Hive Metastore: [http://localhost:9083](http://localhost:9083)
    - Hive Server: [http://localhost:10000](http://localhost:10000)
    - Spark Master: [http://localhost:8080](http://localhost:8080)
    - Spark Worker: [http://localhost:8081](http://localhost:8081)
    - HBase Master: [http://localhost:16010](http://localhost:16010)
    - HBase Regionserver: [http://localhost:16030](http://localhost:16030)
    - Zookeeper: localhost:2181

3.  **Run Verification Scripts**:
    To run the test scripts, first mount or copy them into the appropriate containers:
    ```bash
    # Test Hive Integration (run in spark-master container)
    docker cp scripts/test_hive.py spark-master:/test_hive.py
    docker exec -it spark-master /opt/spark/bin/spark-submit /test_hive.py

    # Test HBase Integration (run in hbase-master container)
    docker cp scripts/test_hbase.sh hbase-master:/test_hbase.sh
    docker exec -it hbase-master bash /test_hbase.sh
    ```

4.  **Access Shells**:
    ```bash
    # Hadoop Namenode
    docker exec -it namenode bash

    # Hive Server
    docker exec -it hive-server bash

    # Spark Master
    docker exec -it spark-master bash

    # HBase Master
    docker exec -it hbase-master bash
    ```

### Components
- **Hadoop Namenode/Datanode**: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 / bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
- **Hive Metastore/Server**: bde2020/hive:2.3.2-postgresql-metastore
- **HBase Master/Regionserver**: harisekhon/hbase
- **Spark Master/Worker**: bde2020/spark-master:3.1.1-hadoop3.2 / bde2020/spark-worker:3.1.1-hadoop3.2
- **Zookeeper**: zookeeper:3.5
- **PostgreSQL**: postgres:12 (for Hive metastore)

## Project Structure
- `config/`: Configuration files for Hadoop, Hive, etc.
- `docker/`: Unused folder (intended for custom Docker builds, not currently implemented).
- `initdb/`: Initialization scripts for HDFS and PostgreSQL.
- `scripts/`: Test and utility scripts for Hive and HBase.
