"""
Benchmark complet pour HBase
- Mesure écriture (min/max/moyen sur plusieurs itérations)
- Mesure lecture complète
- Mesure scan HBase
- Conversion en DataFrame Spark
- Requêtes Spark SQL (filter, groupBy, count)
- Stockage métriques dans perf.hbase_metrics
"""
import time
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum
from pyspark import SparkContext

# Import de la configuration
sys.path.append('.')
try:
    from benchmark_config import *
except ImportError:
    # Valeurs par défaut si le fichier de config n'est pas trouvé
    CSV_HDFS_PATH = "hdfs://namenode:8020/user/data/sample_sales.csv"
    HBASE_TABLE = "sales"
    HBASE_COLUMN_FAMILY = "cf"
    # Par défaut, utiliser le ZooKeeper embarqué dans le conteneur hbase-master
    HBASE_ZK_QUORUM = "hbase-master"
    HIVE_DATABASE = "perf"
    HIVE_TABLE_HBASE_METRICS = "hbase_metrics"
    BENCHMARK_ITERATIONS = 3

def now_ms():
    """Retourne le temps actuel en millisecondes"""
    return int(time.time() * 1000)

def measure_time(func, *args, **kwargs):
    """Mesure le temps d'exécution d'une fonction"""
    t0 = now_ms()
    result = func(*args, **kwargs)
    t1 = now_ms()
    return result, (t1 - t0)

def measure_multiple_times(func, iterations, *args, **kwargs):
    """Mesure le temps d'exécution plusieurs fois et retourne min/max/moyen"""
    times = []
    results = []
    
    for i in range(iterations):
        result, elapsed = measure_time(func, *args, **kwargs)
        times.append(elapsed)
        results.append(result)
        print(f"  Itération {i+1}/{iterations}: {elapsed} ms")
    
    return {
        "min": min(times),
        "max": max(times),
        "avg": sum(times) / len(times),
        "times": times,
        "result": results[0]  # Retourne le résultat de la première itération
    }

print("=" * 60)
print("BENCHMARK HBASE - DÉMARRAGE")
print("=" * 60)

# Initialisation Spark avec support Hive
spark = SparkSession.builder \
    .appName("benchmark_hbase") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
jvm = sc._jvm

# Configuration HBase
hbase_conf = jvm.org.apache.hadoop.hbase.HBaseConfiguration.create()
hbase_conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM)
hbase_conf.set("hbase.zookeeper.property.clientPort", "2181")

print("\nConfiguration HBase:")
print(f"  Zookeeper: {HBASE_ZK_QUORUM}")
print(f"  Table: {HBASE_TABLE}")
print(f"  Column Family: {HBASE_COLUMN_FAMILY}")

# ============================================================
# 1. LECTURE DU CSV
# ============================================================
print("\n" + "=" * 60)
print("1. LECTURE DU CSV DEPUIS HDFS")
print("=" * 60)

def read_csv():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_HDFS_PATH)
    row_count = df.count()
    return df, row_count

result, csv_read_time = measure_time(read_csv)
df_csv, row_count = result

print(f"✓ CSV lu: {row_count} lignes")
print(f"  Temps: {csv_read_time} ms")
df_csv.show(5, truncate=False)

# ============================================================
# 2. ÉCRITURE DANS HBASE
# ============================================================
print("\n" + "=" * 60)
print("2. ÉCRITURE DANS HBASE")
print("=" * 60)

def write_to_hbase():
    """Écrit les données dans HBase"""
    # Préparer les données avec rowkey
    df_with_rowkey = df_csv.withColumn("rowkey", col("id").cast("string"))
    data = df_with_rowkey.collect()
    
    # Connexion HBase
    conn = jvm.org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbase_conf)
    table = conn.getTable(jvm.org.apache.hadoop.hbase.TableName.valueOf(HBASE_TABLE))
    bytes_util = jvm.org.apache.hadoop.hbase.util.Bytes
    
    try:
        # Supprimer les données existantes (pour repartir de zéro)
        admin = conn.getAdmin()
        if admin.tableExists(jvm.org.apache.hadoop.hbase.TableName.valueOf(HBASE_TABLE)):
            scan = jvm.org.apache.hadoop.hbase.client.Scan()
            scanner = table.getScanner(scan)
            delete_list = jvm.java.util.ArrayList()
            for result in scanner:
                delete = jvm.org.apache.hadoop.hbase.client.Delete(result.getRow())
                delete_list.add(delete)
            if delete_list.size() > 0:
                table.delete(delete_list)
        admin.close()
        
        # Écrire les nouvelles données
        for row in data:
            rk = str(row['rowkey'])
            put = jvm.org.apache.hadoop.hbase.client.Put(bytes_util.toBytes(rk))
            put.addColumn(bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
                         bytes_util.toBytes("id"), 
                         bytes_util.toBytes(str(row['id'])))
            put.addColumn(bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
                         bytes_util.toBytes("product"), 
                         bytes_util.toBytes(str(row['product'])))
            put.addColumn(bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
                         bytes_util.toBytes("amount"), 
                         bytes_util.toBytes(str(row['amount'])))
            put.addColumn(bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
                         bytes_util.toBytes("date"), 
                         bytes_util.toBytes(str(row['date'])))
            table.put(put)
        
        return len(data)
    finally:
        table.close()
        conn.close()

print(f"Exécution de {BENCHMARK_ITERATIONS} itérations d'écriture...")
write_metrics = measure_multiple_times(write_to_hbase, BENCHMARK_ITERATIONS)

print(f"\n✓ Écriture terminée:")
print(f"  Temps min: {write_metrics['min']} ms")
print(f"  Temps max: {write_metrics['max']} ms")
print(f"  Temps moyen: {write_metrics['avg']:.2f} ms")
print(f"  Lignes écrites: {write_metrics['result']}")

# ============================================================
# 3. LECTURE COMPLÈTE DEPUIS HBASE (SCAN)
# ============================================================
print("\n" + "=" * 60)
print("3. LECTURE COMPLÈTE DEPUIS HBASE (SCAN)")
print("=" * 60)

def scan_hbase():
    """Effectue un scan complet de la table HBase"""
    conn = jvm.org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbase_conf)
    table = conn.getTable(jvm.org.apache.hadoop.hbase.TableName.valueOf(HBASE_TABLE))
    scan = jvm.org.apache.hadoop.hbase.client.Scan()
    scanner = table.getScanner(scan)
    
    count = 0
    for result in scanner:
        count += 1
    
    scanner.close()
    table.close()
    conn.close()
    return count

print(f"Exécution de {BENCHMARK_ITERATIONS} itérations de scan...")
scan_metrics = measure_multiple_times(scan_hbase, BENCHMARK_ITERATIONS)

print(f"\n✓ Scan terminé:")
print(f"  Temps min: {scan_metrics['min']} ms")
print(f"  Temps max: {scan_metrics['max']} ms")
print(f"  Temps moyen: {scan_metrics['avg']:.2f} ms")
print(f"  Lignes lues: {scan_metrics['result']}")

# ============================================================
# 4. CONVERSION EN DATAFRAME SPARK
# ============================================================
print("\n" + "=" * 60)
print("4. CONVERSION EN DATAFRAME SPARK")
print("=" * 60)

def hbase_to_dataframe():
    """Convertit les données HBase en DataFrame Spark"""
    conn = jvm.org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbase_conf)
    table = conn.getTable(jvm.org.apache.hadoop.hbase.TableName.valueOf(HBASE_TABLE))
    scan = jvm.org.apache.hadoop.hbase.client.Scan()
    scanner = table.getScanner(scan)
    bytes_util = jvm.org.apache.hadoop.hbase.util.Bytes
    
    rows = []
    for result in scanner:
        rowkey = bytes_util.toString(result.getRow())
        id_val = bytes_util.toString(result.getValue(
            bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
            bytes_util.toBytes("id")))
        product = bytes_util.toString(result.getValue(
            bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
            bytes_util.toBytes("product")))
        amount = bytes_util.toString(result.getValue(
            bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
            bytes_util.toBytes("amount")))
        date = bytes_util.toString(result.getValue(
            bytes_util.toBytes(HBASE_COLUMN_FAMILY), 
            bytes_util.toBytes("date")))
        
        rows.append((int(id_val) if id_val else None, 
                    product if product else None,
                    float(amount) if amount else None,
                    date if date else None))
    
    scanner.close()
    table.close()
    conn.close()
    
    # Créer DataFrame
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True)
    ])
    
    df = spark.createDataFrame(rows, schema)
    count = df.count()
    return df, count

df_hbase, hbase_row_count = measure_time(hbase_to_dataframe)
df_conv_time = measure_time(hbase_to_dataframe)[1]

print(f"✓ Conversion terminée:")
print(f"  Temps: {df_conv_time} ms")
print(f"  Lignes converties: {hbase_row_count}")
df_hbase.show(5, truncate=False)

# Enregistrer comme table temporaire pour Spark SQL
df_hbase.createOrReplaceTempView("hbase_sales")

# ============================================================
# 5. REQUÊTES SPARK SQL
# ============================================================
print("\n" + "=" * 60)
print("5. REQUÊTES SPARK SQL")
print("=" * 60)

# 5.1. SELECT *
def query_select_all():
    return spark.sql("SELECT * FROM hbase_sales").count()

print("\n5.1. SELECT * FROM hbase_sales")
select_metrics = measure_multiple_times(query_select_all, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {select_metrics['avg']:.2f} ms")

# 5.2. FILTER (amount > 100)
def query_filter():
    return spark.sql("SELECT * FROM hbase_sales WHERE amount > 100").count()

print("\n5.2. SELECT * FROM hbase_sales WHERE amount > 100")
filter_metrics = measure_multiple_times(query_filter, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {filter_metrics['avg']:.2f} ms")
result = spark.sql("SELECT * FROM hbase_sales WHERE amount > 100")
result.show()

# 5.3. GROUP BY product
def query_groupby():
    return spark.sql("SELECT product, COUNT(*) as count, SUM(amount) as total FROM hbase_sales GROUP BY product").collect()

print("\n5.3. SELECT product, COUNT(*), SUM(amount) FROM hbase_sales GROUP BY product")
groupby_metrics = measure_multiple_times(query_groupby, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {groupby_metrics['avg']:.2f} ms")
result = spark.sql("SELECT product, COUNT(*) as count, SUM(amount) as total FROM hbase_sales GROUP BY product")
result.show()

# 5.4. COUNT
def query_count():
    return spark.sql("SELECT COUNT(*) as total FROM hbase_sales").collect()[0]['total']

print("\n5.4. SELECT COUNT(*) FROM hbase_sales")
count_metrics = measure_multiple_times(query_count, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {count_metrics['avg']:.2f} ms")

# ============================================================
# 6. STOCKAGE DES MÉTRIQUES DANS HIVE
# ============================================================
print("\n" + "=" * 60)
print("6. STOCKAGE DES MÉTRIQUES DANS HIVE")
print("=" * 60)

# Créer la base de données si elle n'existe pas
spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")

# Préparer les métriques
metrics_data = [{
    "format": "hbase",
    "operation": "csv_read",
    "time_ms": csv_read_time,
    "time_min_ms": csv_read_time,
    "time_max_ms": csv_read_time,
    "time_avg_ms": csv_read_time,
    "rows_processed": row_count,
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "write",
    "time_ms": write_metrics['avg'],
    "time_min_ms": write_metrics['min'],
    "time_max_ms": write_metrics['max'],
    "time_avg_ms": write_metrics['avg'],
    "rows_processed": write_metrics['result'],
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "scan",
    "time_ms": scan_metrics['avg'],
    "time_min_ms": scan_metrics['min'],
    "time_max_ms": scan_metrics['max'],
    "time_avg_ms": scan_metrics['avg'],
    "rows_processed": scan_metrics['result'],
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "dataframe_conversion",
    "time_ms": df_conv_time,
    "time_min_ms": df_conv_time,
    "time_max_ms": df_conv_time,
    "time_avg_ms": df_conv_time,
    "rows_processed": hbase_row_count,
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "sql_select_all",
    "time_ms": select_metrics['avg'],
    "time_min_ms": select_metrics['min'],
    "time_max_ms": select_metrics['max'],
    "time_avg_ms": select_metrics['avg'],
    "rows_processed": row_count,
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "sql_filter",
    "time_ms": filter_metrics['avg'],
    "time_min_ms": filter_metrics['min'],
    "time_max_ms": filter_metrics['max'],
    "time_avg_ms": filter_metrics['avg'],
    "rows_processed": filter_metrics['result'][0] if filter_metrics['result'] else 0,
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "sql_groupby",
    "time_ms": groupby_metrics['avg'],
    "time_min_ms": groupby_metrics['min'],
    "time_max_ms": groupby_metrics['max'],
    "time_avg_ms": groupby_metrics['avg'],
    "rows_processed": len(groupby_metrics['result']),
    "timestamp": int(time.time())
}, {
    "format": "hbase",
    "operation": "sql_count",
    "time_ms": count_metrics['avg'],
    "time_min_ms": count_metrics['min'],
    "time_max_ms": count_metrics['max'],
    "time_avg_ms": count_metrics['avg'],
    "rows_processed": count_metrics['result'],
    "timestamp": int(time.time())
}]

metrics_df = spark.createDataFrame(metrics_data)

# Créer ou remplacer la table
metrics_df.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE_HBASE_METRICS}")

print(f"✓ Métriques stockées dans {HIVE_DATABASE}.{HIVE_TABLE_HBASE_METRICS}")
metrics_df.show(truncate=False)

# ============================================================
# RÉSUMÉ
# ============================================================
print("\n" + "=" * 60)
print("RÉSUMÉ DU BENCHMARK HBASE")
print("=" * 60)
print(f"Lignes traitées: {row_count}")
print(f"\nTemps d'écriture (moyen): {write_metrics['avg']:.2f} ms")
print(f"Temps de scan (moyen): {scan_metrics['avg']:.2f} ms")
print(f"Temps de conversion DataFrame: {df_conv_time} ms")
print(f"Temps requête SELECT (moyen): {select_metrics['avg']:.2f} ms")
print(f"Temps requête FILTER (moyen): {filter_metrics['avg']:.2f} ms")
print(f"Temps requête GROUP BY (moyen): {groupby_metrics['avg']:.2f} ms")
print(f"Temps requête COUNT (moyen): {count_metrics['avg']:.2f} ms")

spark.stop()
print("\n✓ Benchmark HBase terminé")

