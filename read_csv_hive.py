from pyspark.sql import SparkSession

# Create SparkSession with explicit Hadoop configuration
spark = SparkSession.builder \
    .appName("CSVHiveReader") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.fs.default.name", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# CRITICAL: Force set the Hadoop configuration AFTER SparkSession creation
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://namenode:8020")
hadoop_conf.set("fs.default.name", "hdfs://namenode:8020")

print("=== Spark Session Created ===")
print("Hadoop fs.defaultFS:", hadoop_conf.get("fs.defaultFS"))

# Method 1: Create external Hive table pointing to CSV
print("=== Creating External Hive Table ===")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_csv_external (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/data/sample_sales/'
TBLPROPERTIES ('skip.header.line.count'='1')
""")

print("=== Querying External Table ===")
result = spark.sql("SELECT * FROM sales_csv_external")
result.show()

# Method 2: Read CSV and write as Parquet to HDFS, then create external table
print("=== Reading CSV Directly ===")
df = spark.read.csv(
    "hdfs://namenode:8020/user/data/sample_sales/",
    header=True,
    inferSchema=True
)

print("DataFrame schema:", df.schema)
print("Row count:", df.count())

print("=== Writing DataFrame to HDFS as Parquet ===")
# Write directly to HDFS with FULL path
output_path = "hdfs://namenode:8020/user/hive/warehouse/sales_managed"
df.write.mode("overwrite").parquet(output_path)
print(f"Data written to {output_path}")

print("=== Creating External Hive Table for Parquet Data ===")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_managed (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/sales_managed'
""")

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