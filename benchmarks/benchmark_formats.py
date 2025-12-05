import sys
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def benchmark_formats(input_file, dataset_name):
    # Initialisation Spark
    spark = SparkSession.builder \
        .appName(f"BenchmarkFormats_{dataset_name}") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    print(f"\n=== BENCHMARK: {dataset_name} ({input_file}) ===")

    # 1. Lecture du CSV source
    print("Lecture du fichier source...")
    start_read = time.time()
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    read_source_time = time.time() - start_read
    row_count = df.count()
    print(f"Lignes: {row_count}, Temps lecture source: {read_source_time:.4f}s")

    formats = ["csv", "parquet", "orc", "avro"]
    results = []

    for fmt in formats:
        print(f"\n--- Format: {fmt.upper()} ---")
        
        output_path = f"hdfs://namenode:8020/user/output/{dataset_name}_{fmt}"
        
        # Nettoyage
        path_obj = sc._jvm.org.apache.hadoop.fs.Path(output_path)
        if fs.exists(path_obj):
            fs.delete(path_obj, True)

        # Benchmark Écriture
        start_write = time.time()
        if fmt == "csv":
            df.write.mode("overwrite").option("header", "true").csv(output_path)
        elif fmt == "avro":
            df.write.format("avro").mode("overwrite").save(output_path)
        else:
            df.write.format(fmt).mode("overwrite").save(output_path)
        write_time = time.time() - start_write
        print(f"Écriture: {write_time:.4f}s")

        # Taille du fichier
        size_bytes = fs.getContentSummary(path_obj).getLength()
        size_mb = size_bytes / (1024 * 1024)
        print(f"Taille: {size_mb:.2f} MB")

        # Benchmark Lecture
        start_read = time.time()
        if fmt == "csv":
            count = spark.read.csv(output_path, header=True).count()
        elif fmt == "avro":
            count = spark.read.format("avro").load(output_path).count()
        else:
            count = spark.read.format(fmt).load(output_path).count()
        read_time = time.time() - start_read
        print(f"Lecture (Count): {read_time:.4f}s")

        results.append({
            "dataset": dataset_name,
            "format": fmt,
            "rows": row_count,
            "write_time_s": float(write_time),
            "read_time_s": float(read_time),
            "size_mb": float(size_mb)
        })

    # Sauvegarde des résultats dans Hive
    print("\nSauvegarde des résultats dans Hive...")
    results_df = spark.createDataFrame(results)
    
    # Création de la table si elle n'existe pas
    spark.sql("""
        CREATE TABLE IF NOT EXISTS perf.format_benchmark_results (
            dataset STRING,
            format STRING,
            rows LONG,
            write_time_s DOUBLE,
            read_time_s DOUBLE,
            size_mb DOUBLE
        )
        STORED AS PARQUET
    """)
    
    results_df.write.mode("append").insertInto("perf.format_benchmark_results")
    print("✓ Résultats sauvegardés dans perf.format_benchmark_results")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--dataset-name", required=True)
    args = parser.parse_args()
    
    benchmark_formats(args.input_file, args.dataset_name)
