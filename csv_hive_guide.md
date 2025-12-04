# Step-by-Step Guide: Reading CSV Files with Spark SQL and Hive

This guide provides detailed steps to read a CSV file using Spark SQL with Hive support in your Big Data environment.

## Prerequisites

- Docker and Docker Compose installed
- All services in `docker-compose.yml` are properly configured and can start successfully
- HDFS namenode and datanode are healthy and communicating
- Hive metastore is running and accessible
- Spark master and workers are operational

Verify environment health:

```bash
docker-compose ps
```

All services should show "Up" status, and health checks should pass for namenode, datanode, zookeeper, hive-metastore-postgresql, and hive-metastore.

## Step 1: Start the Environment

Ensure all services are running:

```bash
docker-compose up -d
```

Verify services are healthy:

```bash
docker-compose ps
```

Expected: All services should show "Up" and "healthy" where applicable.

## Step 2: Prepare a Sample CSV File

Create a sample CSV file on your host machine:

```bash
# Create a sample CSV file
cat > sample_sales.csv << EOF
id,product,amount,date
1,Laptop,1200.50,2023-01-01
2,Mouse,25.99,2023-01-02
3,Keyboard,75.00,2023-01-03
4,Monitor,300.00,2023-01-04
5,Headphones,150.25,2023-01-05
EOF
```

## Step 3: Upload CSV to HDFS

Copy the CSV file to the namenode container and upload to HDFS:

```bash
# Copy CSV file to namenode container
docker cp sample_sales.csv namenode:/sample_sales.csv

# Create directory in HDFS
docker exec -it namenode hdfs dfs -mkdir -p /user/data

# Upload the CSV file to HDFS
docker exec -it namenode hdfs dfs -put /sample_sales.csv /user/data/
```

Verify upload:

```bash
docker exec -it namenode hdfs dfs -ls /user/data/
docker exec -it namenode hdfs dfs -cat /user/data/sample_sales.csv
```

Expected output:
```
-rw-r--r-- 3 root supergroup 123 2025-12-01 20:37 /user/data/sample_sales.csv

id,product,amount,date
1,Laptop,1200.50,2023-01-01
2,Mouse,25.99,2023-01-02
3,Keyboard,75.00,2023-01-03
4,Monitor,300.00,2023-01-04
5,Headphones,150.25,2023-01-05
```

## Step 4: Create PySpark Script

Create a Python script for reading the CSV with Spark SQL and Hive:

```bash
cat > read_csv_hive.py << 'EOF'
from pyspark.sql import SparkSession
import time

# Create SparkSession with Hive support
spark = SparkSession.builder \
    .appName("CSVHiveReader") \
    .enableHiveSupport() \
    .getOrCreate()

print("=== Spark Session Created ===")

# Method 1: Create external Hive table pointing to CSV
print("=== Creating External Hive Table ===")
spark.sql("DROP TABLE IF EXISTS sales_csv_external")
spark.sql("""
CREATE EXTERNAL TABLE sales_csv_external (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/data/'
TBLPROPERTIES ('skip.header.line.count'='1')
""")

print("=== Querying External Table ===")
result = spark.sql("SELECT * FROM sales_csv_external")
result.show()

# Method 2: Read CSV directly and create managed Hive table
print("=== Reading CSV Directly ===")
df = spark.read.csv("hdfs://namenode:8020/user/data/sample_sales.csv", header=True, inferSchema=True)
print(f"DataFrame schema: {df.schema}")
print(f"Row count: {df.count()}")

print("=== Creating Managed Hive Table ===")
df.write.mode("overwrite").saveAsTable("sales_managed")

print("=== Querying Managed Table ===")
managed_result = spark.sql("SELECT * FROM sales_managed")
managed_result.show()

# Show databases and tables
print("=== Available Databases ===")
spark.sql("SHOW DATABASES").show()

print("=== Tables in Default Database ===")
spark.sql("SHOW TABLES").show()

print("=== Test Completed Successfully ===")

spark.stop()
EOF
```

## Step 5: Run the PySpark Script

Copy and run the script in the Spark container:

```bash
# Copy script to Spark master
docker cp read_csv_hive.py spark-master-new:/read_csv_hive.py

# Run the script
docker exec -it spark-master-new /spark/bin/spark-submit /read_csv_hive.py
/spark/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.1.1 /read_csv_hive.py
```

Note: If the path is different, check with `find / -name spark-submit 2>/dev/null` inside the container.

## Step 6: Verify Results

Check that the tables were created in Hive and data is accessible:

### Via Spark Shell

```bash
# Verify tables and data via Spark shell
docker exec -it spark-master-new /spark/bin/spark-shell -i /dev/stdin <<EOF
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE sales_csv_external").show()
spark.sql("DESCRIBE sales_managed").show()
spark.sql("SELECT COUNT(*) FROM sales_csv_external").show()
spark.sql("SELECT COUNT(*) FROM sales_managed").show()
spark.sql("SELECT * FROM sales_csv_external ORDER BY id").show()
spark.sql("SELECT product, SUM(amount) as total_sales FROM sales_managed GROUP BY product").show()
:q
EOF
```

Expected output:
```
+--------------------+-----------+
|           tableName|isTemporary|
+--------------------+-----------+
|   sales_csv_external|      false|
|        sales_managed|      false|
+--------------------+-----------+

+-------+---------+-------+
|col_name|data_type|comment|
+-------+---------+-------+
|     id|      int|       |
|product|   string|       |
| amount|   double|       |
|   date|   string|       |
+-------+---------+-------+

+-------+---------+-------+
|col_name|data_type|comment|
+-------+---------+-------+
|     id|      int|       |
|product|   string|       |
| amount|   double|       |
|   date|   string|       |
+-------+---------+-------+

+--------+
|count(1)|
+--------+
|       5|
+--------+

+--------+
|count(1)|
+--------+
|       5|
+--------+

+---+---------+------+----------+
| id|  product|amount|      date|
+---+---------+------+----------+
|  1|   Laptop|1200.5|2023-01-01|
|  2|    Mouse| 25.99|2023-01-02|
|  3| Keyboard|  75.0|2023-01-03|
|  4|  Monitor| 300.0|2023-01-04|
|  5|Headphones|150.25|2023-01-05|
+---+---------+------+----------+

+----------+-----------+
|   product|total_sales|
+----------+-----------+
|Headphones|     150.25|
|   Monitor|      300.0|
| Keyboard|       75.0|
|    Mouse|      25.99|
|   Laptop|    1200.5|
+----------+-----------+
```

### Via Hive CLI

Connect directly to Hive for verification:

```bash
# Access Hive CLI
docker exec -it hive-metastore /opt/hive/bin/hive -e "
SHOW DATABASES;
USE default;
SHOW TABLES;
DESCRIBE sales_csv_external;
DESCRIBE sales_managed;
SELECT * FROM sales_csv_external ORDER BY id;
SELECT product, SUM(amount) as total_sales FROM sales_managed GROUP BY product;
"
```

Expected output similar to above.

### Via Beeline (JDBC Connection)

Test JDBC connectivity:

```bash
# Connect via Beeline
docker exec -it hive-metastore /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "
SHOW TABLES;
SELECT COUNT(*) FROM sales_csv_external;
SELECT * FROM sales_managed WHERE amount > 100;
"
```

Expected output:
```
+--------------------+-----------+
|           tableName|isTemporary|
+--------------------+-----------+
|   sales_csv_external|      false|
|        sales_managed|      false|
+--------------------+-----------+

+--------+
|count(1)|
+--------+
|       5|
+--------+

+---+---------+------+----------+
| id|  product|amount|      date|
+---+---------+------+----------+
|  1|   Laptop|1200.5|2023-01-01|
|  4|  Monitor| 300.0|2023-01-04|
|  5|Headphones|150.25|2023-01-05|
+---+---------+------+----------+
```

### Verify HDFS Storage

Check that data is stored in HDFS:

```bash
# Check HDFS files
docker exec -it namenode hdfs dfs -ls -R /user/hive/warehouse/
docker exec -it namenode hdfs dfs -cat /user/data/sample_sales.csv
```

## Step 7: Clean Up (Optional)

To remove the test tables:

```bash
docker exec -it spark-master-new /spark/bin/spark-shell -i /dev/stdin <<EOF
spark.sql("DROP TABLE sales_csv_external")
spark.sql("DROP TABLE sales_managed")
:q
EOF
```

## Troubleshooting

- **Services not starting**: Check Docker logs with `docker-compose logs <service_name>`. Ensure ports are not in use and configurations are correct.
- **Datanode not connecting to Namenode**: Verify `CORE_CONF_fs_defaultFS` is set to `hdfs://namenode:8020` in datanode environment. Check that namenode healthcheck passes before datanode starts.
- **Hive metastore connection issues**: Ensure PostgreSQL is healthy and `hive-site.xml` has correct JDBC settings.
- **Spark can't connect to Hive**: Ensure `hive-metastore` is healthy and `hive-site.xml` is correctly mounted. Check Spark logs for metastore URI errors.
- **CSV not found**: Check HDFS path with `hdfs dfs -ls /user/data/`. Ensure file was uploaded correctly and permissions allow access.
- **Schema inference fails**: Specify schema manually in `spark.read.csv()` or check CSV format for consistency.
- **HDFS permissions**: Use `hdfs dfs -chmod 755 /user/data/` if access denied.
- **Port conflicts**: Verify no other services use ports 9870, 9864, 9083, 10000, etc.

This guide demonstrates reading CSV files using Spark SQL with Hive, which you can extend for your format comparison tasks.