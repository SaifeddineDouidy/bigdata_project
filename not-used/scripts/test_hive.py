from pyspark.sql import SparkSession
import os

# Ensure we can see the console output
spark = SparkSession.builder \
    .appName("HiveTest") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()


print("Creating Hive table...")
spark.sql("CREATE TABLE IF NOT EXISTS test_hive (key INT, value STRING)")
print("Inserting data...")
spark.sql("INSERT INTO test_hive VALUES (1, 'Hello'), (2, 'World')")
print("Selecting data...")
spark.sql("SELECT * FROM test_hive").show()

print("Hive Test Completed.")
spark.stop()
