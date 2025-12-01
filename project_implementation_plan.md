# Big Data Project Implementation Plan

This document outlines the detailed steps to complete the project tasks: comparing Apache Avro, ORC, and Parquet formats using Hive, Spark SQL, and Spark ML.

## Phase 1: Environment Preparation & Data Ingestion

### 1.1 Verify Environment
- Run the checks in `environment_verification.md` to ensure all services are healthy.
- Ensure `spark-avro` package is available (will be used via `--packages` flag).

### 1.2 Prepare Dataset
- **Action:** Download or generate a CSV dataset (e.g., a large sales or sensor data file) to be used for benchmarking.
- **Location:** Place the CSV file in the `data/` directory (mounted to `/data_host` in containers) or upload to HDFS.
- **Command:**
  ```bash
  # Example: Upload local CSV to HDFS
  docker exec -it namenode hdfs dfs -put /hadoop/dfs/data/dataset.csv /user/data/
  ```

## Phase 2: Hive & Spark SQL Integration

### 2.1 Use Hive within Spark SQL
- **Goal:** Configure Spark to connect to the existing Hive Metastore.
- **Verification:**
  - Ensure `hive-site.xml` is correctly mounted in Spark containers.
  - Run a test query from Spark to list Hive tables.

### 2.2 Read CSV using Spark SQL with Hive
- **Task:** Create a Hive table pointing to the CSV data using Spark SQL.
- **Code Snippet (PySpark):**
  ```python
  spark.sql("CREATE TABLE IF NOT EXISTS sales_csv USING hive OPTIONS(fileFormat 'csv', header 'true', inferSchema 'true') LOCATION '/user/data/sales_csv'")
  ```

## Phase 3: Format Comparison (Avro, ORC, Parquet)

### 3.1 Convert Data to Formats
- **Task:** Read the CSV data and write it back to HDFS in Avro, ORC, and Parquet formats.
- **Code Logic:**
  ```python
  df = spark.read.csv("/user/data/dataset.csv", header=True, inferSchema=True)
  
  # Write Parquet
  df.write.format("parquet").save("/user/data/sales_parquet")
  
  # Write ORC
  df.write.format("orc").save("/user/data/sales_orc")
  
  # Write Avro (requires spark-avro package)
  df.write.format("avro").save("/user/data/sales_avro")
  ```

### 3.2 Benchmark Read/Write Performance
- **Task:** Measure and record the time taken for:
  - **Write:** Writing the DataFrame to each format.
  - **Read:** Reading the data back from each format into a DataFrame.
  - **Query:** Running a standard aggregation query (e.g., `SELECT count(*) FROM ... WHERE ...`) on each format.
- **Output:** A comparison table (can be printed to console or saved).

## Phase 4: HBase vs. Parquet Comparison

### 4.1 Ingest Data into HBase
- **Task:** Write the same dataset into an HBase table.
- **Method:** Use the `HBaseTableCatalog` with Spark (requires `shc-core` or similar connector) or standard HBase shell/API.

### 4.2 Benchmark Performance
- **Task:** Compare Parquet (from Phase 3) against HBase for:
  - **Write Speed:** Time to bulk load data.
  - **Read Speed:** Time to scan the full dataset.
  - **Query Speed:** Time to perform a specific lookup or aggregation.

## Phase 5: Advanced Data Handling

### 5.1 CSV to Parquet via Hive
- **Task:** Read a CSV file using Spark (Hive context) and serialize it directly to Parquet.
- **Command:**
  ```python
  spark.sql("CREATE TABLE sales_parquet_hive STORED AS PARQUET AS SELECT * FROM sales_csv")
  ```

## Phase 6: Spark ML & Performance Tracking

### 6.1 Spark ML Pipeline
- **Task:** Run a Machine Learning algorithm (e.g., Linear Regression or K-Means) on the dataset.
- **Goal:** Measure training time and accuracy metrics.

### 6.2 Comparative Analysis
- **Task:** Run the same ML algorithm on data sourced from the different formats (CSV, Parquet, ORC, Avro).
- **Metric:** Compare data loading time + training time.

### 6.3 Store Results in Hive
- **Task:** Create a Hive table `ml_performance_results` and insert the benchmark metrics.
- **Schema:** `(format STRING, operation STRING, time_taken DOUBLE, accuracy DOUBLE)`

## Next Steps
1. **Approve this plan.**
2. **Switch to Code mode** to start implementing the scripts for Phase 2 and 3.