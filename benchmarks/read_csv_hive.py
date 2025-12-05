from pyspark.sql import SparkSession
import time

# ============================================================
# 1. CREATE SPARK SESSION WITH HIVE + HDFS CONFIG
# ============================================================
spark = SparkSession.builder \
    .appName("BigDataProject-AllTasks") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# Reapply Hadoop config explicitly
hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.defaultFS", "hdfs://namenode:8020")

print("\n=== Spark + Hive Environment Ready ===")
print("fs.defaultFS:", hconf.get("fs.defaultFS"))

# ============================================================
# 2. READ CSV FROM HDFS USING SPARK + HIVE
# ============================================================
csv_path = "hdfs://namenode:8020/user/data/sample_sales/"

print("\n=== Reading CSV from HDFS ===")
df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.printSchema()
print("Row count:", df.count())

# ============================================================
# 3. CREATE HIVE EXTERNAL TABLE FOR CSV
# ============================================================
print("\n=== Creating External Hive Table for CSV ===")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_csv_external (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/data/sample_sales/'
TBLPROPERTIES ('skip.header.line.count'='1')
""")

print("\n=== Querying sales_csv_external ===")
spark.sql("SELECT * FROM sales_csv_external LIMIT 5").show()

# ============================================================
# 4. WRITE IN PARQUET, ORC, AVRO (Benchmark)
# ============================================================
formats = ["parquet", "orc", "avro"]
paths = {
    "parquet": "hdfs://namenode:8020/user/hive/warehouse/sales_parquet",
    "orc": "hdfs://namenode:8020/user/hive/warehouse/sales_orc",
    "avro": "hdfs://namenode:8020/user/hive/warehouse/sales_avro"
}

write_times = {}
read_times = {}

for f in formats:
    print(f"\n=== Writing {f.upper()} format ===")
    start = time.time()
    df.write.mode("overwrite").format(f).save(paths[f])
    write_times[f] = time.time() - start

    print(f"Write time {f}: {write_times[f]:.4f}s")

    # Read benchmark
    start = time.time()
    _ = spark.read.format(f).load(paths[f]).count()
    read_times[f] = time.time() - start

    print(f"Read time {f}: {read_times[f]:.4f}s")

# ============================================================
# 5. CREATE HIVE TABLES FOR PARQUET, ORC, AVRO
# ============================================================
print("\n=== Creating Hive Tables for Parquet, ORC, Avro ===")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_parquet (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/sales_parquet'
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_orc (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
STORED AS ORC
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/sales_orc'
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS sales_avro (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
STORED AS AVRO
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/sales_avro'
""")

print("\n=== Showing Sample Rows From Hive Tables ===")
spark.sql("SELECT * FROM sales_parquet LIMIT 5").show()
spark.sql("SELECT * FROM sales_orc LIMIT 5").show()
spark.sql("SELECT * FROM sales_avro LIMIT 5").show()

# ============================================================
# 6. PRINT BENCHMARK SUMMARY
# ============================================================
print("\n======= WRITE TIME (seconds) =======")
for f in formats:
    print(f"{f.upper():8} : {write_times[f]:.4f}s")

print("\n======= READ TIME (seconds) =======")
for f in formats:
    print(f"{f.upper():8} : {read_times[f]:.4f}s")

print("\n=== Environment Setup + File Format Benchmarks DONE ===")

spark.stop()
