from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShowMetrics") \
    .enableHiveSupport() \
    .getOrCreate()

print("\n=== HBASE METRICS ===")
spark.sql("SELECT * FROM perf.hbase_metrics").show(truncate=False)

print("\n=== PARQUET METRICS ===")
spark.sql("SELECT * FROM perf.parquet_metrics").show(truncate=False)

spark.stop()
