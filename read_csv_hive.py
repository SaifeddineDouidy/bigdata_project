from pyspark.sql import SparkSession
import time

# Create SparkSession with Hive support

# spark = (
#     SparkSession.builder
#     .appName("CSVHiveReader")
#     .enableHiveSupport()
#     .config("spark.sql.warehouse.dir", "/hive/warehouse")
#     .config("hive.metastore.warehouse.dir", "/hive/warehouse")
#     .getOrCreate()
# )

spark = SparkSession.builder \
    .appName("CSVHiveReader") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()


print("=== Spark Session Created ===")

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

# Method 2: Read CSV directly and create managed Hive table
print("=== Reading CSV Directly ===")
df = spark.read.csv(
    "hdfs://namenode:8020/user/data/sample_sales/",
    header=True,
    inferSchema=True
)

print("DataFrame schema:", df.schema)
print("Row count:", df.count())

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
