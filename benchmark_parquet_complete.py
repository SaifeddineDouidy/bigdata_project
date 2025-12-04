"""
Benchmark complet pour Parquet
- Mesure écriture (min/max/moyen sur plusieurs itérations)
- Mesure lecture
- Création table Hive externe
- Requêtes Spark SQL complètes
- Calcul taille/compression
- Stockage métriques dans perf.parquet_metrics
"""
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# Import de la configuration
sys.path.append('.')
try:
    from benchmark_config import *
except ImportError:
    # Valeurs par défaut
    CSV_HDFS_PATH = "hdfs://namenode:8020/user/data/sample_sales.csv"
    PARQUET_HDFS_DIR = "hdfs://namenode:8020/user/output/parquet_sample"
    HIVE_DATABASE = "perf"
    HIVE_TABLE_PARQUET_METRICS = "parquet_metrics"
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
        "result": results[0]
    }

print("=" * 60)
print("BENCHMARK PARQUET - DÉMARRAGE")
print("=" * 60)

# Initialisation Spark avec support Hive
spark = SparkSession.builder \
    .appName("benchmark_parquet") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.defaultFS", "hdfs://namenode:8020")

print("\nConfiguration:")
print(f"  HDFS: hdfs://namenode:8020")
print(f"  Parquet directory: {PARQUET_HDFS_DIR}")

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
# 2. ÉCRITURE EN PARQUET
# ============================================================
print("\n" + "=" * 60)
print("2. ÉCRITURE EN PARQUET")
print("=" * 60)

def write_parquet():
    """Écrit les données en format Parquet"""
    df_csv.write.mode("overwrite").parquet(PARQUET_HDFS_DIR)
    return row_count

print(f"Exécution de {BENCHMARK_ITERATIONS} itérations d'écriture...")
write_metrics = measure_multiple_times(write_parquet, BENCHMARK_ITERATIONS)

print(f"\n✓ Écriture terminée:")
print(f"  Temps min: {write_metrics['min']} ms")
print(f"  Temps max: {write_metrics['max']} ms")
print(f"  Temps moyen: {write_metrics['avg']:.2f} ms")
print(f"  Lignes écrites: {write_metrics['result']}")

# ============================================================
# 3. CALCUL DE LA TAILLE DU DOSSIER PARQUET
# ============================================================
print("\n" + "=" * 60)
print("3. CALCUL DE LA TAILLE DU DOSSIER PARQUET")
print("=" * 60)

def get_parquet_size():
    """Calcule la taille totale du dossier Parquet sur HDFS"""
    try:
        # Utiliser l'API HDFS via JVM
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        path = sc._jvm.org.apache.hadoop.fs.Path(PARQUET_HDFS_DIR)
        
        if fs.exists(path):
            # Calculer la taille récursive
            file_status = fs.getFileStatus(path)
            if file_status.isDirectory():
                # Parcourir récursivement
                remote_iterator = fs.listFiles(path, True)
                total_size = 0
                file_count = 0
                while remote_iterator.hasNext():
                    file_status = remote_iterator.next()
                    total_size += file_status.getLen()
                    file_count += 1
                return total_size, file_count
            else:
                return file_status.getLen(), 1
        else:
            return 0, 0
    except Exception as e:
        print(f"  Erreur lors du calcul de la taille: {e}")
        return 0, 0

parquet_size_bytes, parquet_file_count = get_parquet_size()
parquet_size_mb = parquet_size_bytes / (1024 * 1024)

print(f"✓ Taille calculée:")
print(f"  Taille totale: {parquet_size_bytes} bytes ({parquet_size_mb:.4f} MB)")
print(f"  Nombre de fichiers: {parquet_file_count}")

# ============================================================
# 4. LECTURE DES FICHIERS PARQUET
# ============================================================
print("\n" + "=" * 60)
print("4. LECTURE DES FICHIERS PARQUET")
print("=" * 60)

def read_parquet():
    """Lit les fichiers Parquet"""
    df = spark.read.parquet(PARQUET_HDFS_DIR)
    row_count = df.count()
    return df, row_count

print(f"Exécution de {BENCHMARK_ITERATIONS} itérations de lecture...")
read_result, read_time_first = measure_time(read_parquet)
read_metrics = measure_multiple_times(lambda: read_parquet()[1], BENCHMARK_ITERATIONS)

df_parquet, parquet_row_count = read_result

print(f"\n✓ Lecture terminée:")
print(f"  Temps min: {read_metrics['min']} ms")
print(f"  Temps max: {read_metrics['max']} ms")
print(f"  Temps moyen: {read_metrics['avg']:.2f} ms")
print(f"  Lignes lues: {parquet_row_count}")
df_parquet.show(5, truncate=False)

# ============================================================
# 5. CRÉATION DE LA TABLE HIVE EXTERNE
# ============================================================
print("\n" + "=" * 60)
print("5. CRÉATION DE LA TABLE HIVE EXTERNE")
print("=" * 60)

# Créer la base de données si elle n'existe pas (avec gestion d'erreur)
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
except Exception as e:
    print(f"⚠ Avertissement lors de la création de la base de données: {e}")
    print("  Continuation sans Hive...")

# Supprimer la table si elle existe
try:
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.sales_parquet")
except Exception as e:
    print(f"⚠ Avertissement lors de la suppression de la table: {e}")

# Créer la table externe
create_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_DATABASE}.sales_parquet (
    id INT,
    product STRING,
    amount DOUBLE,
    date STRING
)
STORED AS PARQUET
LOCATION '{PARQUET_HDFS_DIR}'
"""

try:
    spark.sql(create_table_sql)
    print(f"✓ Table Hive créée: {HIVE_DATABASE}.sales_parquet")
    # Vérifier
    spark.sql(f"SELECT COUNT(*) as total FROM {HIVE_DATABASE}.sales_parquet").show()
    use_hive_table = True
except Exception as e:
    print(f"⚠ Avertissement: Impossible de créer la table Hive: {e}")
    print("  Utilisation de la vue temporaire uniquement...")
    use_hive_table = False

# ============================================================
# 6. REQUÊTES SPARK SQL
# ============================================================
print("\n" + "=" * 60)
print("6. REQUÊTES SPARK SQL")
print("=" * 60)

# Enregistrer aussi comme vue temporaire pour comparaison
df_parquet.createOrReplaceTempView("parquet_sales")

# 6.1. SELECT *
def query_select_all():
    if use_hive_table:
        return spark.sql(f"SELECT * FROM {HIVE_DATABASE}.sales_parquet").count()
    else:
        return spark.sql("SELECT * FROM parquet_sales").count()

print("\n6.1. SELECT * FROM sales_parquet")
select_metrics = measure_multiple_times(query_select_all, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {select_metrics['avg']:.2f} ms")
if use_hive_table:
    spark.sql(f"SELECT * FROM {HIVE_DATABASE}.sales_parquet LIMIT 5").show()
else:
    spark.sql("SELECT * FROM parquet_sales LIMIT 5").show()

# 6.2. FILTER (amount > 100)
def query_filter():
    if use_hive_table:
        return spark.sql(f"SELECT * FROM {HIVE_DATABASE}.sales_parquet WHERE amount > 100").count()
    else:
        return spark.sql("SELECT * FROM parquet_sales WHERE amount > 100").count()

print("\n6.2. SELECT * FROM sales_parquet WHERE amount > 100")
filter_metrics = measure_multiple_times(query_filter, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {filter_metrics['avg']:.2f} ms")
if use_hive_table:
    result = spark.sql(f"SELECT * FROM {HIVE_DATABASE}.sales_parquet WHERE amount > 100")
else:
    result = spark.sql("SELECT * FROM parquet_sales WHERE amount > 100")
result.show()

# 6.3. GROUP BY product
def query_groupby():
    if use_hive_table:
        return spark.sql(f"""
            SELECT product, COUNT(*) as count, SUM(amount) as total 
            FROM {HIVE_DATABASE}.sales_parquet 
            GROUP BY product
        """).collect()
    else:
        return spark.sql("""
            SELECT product, COUNT(*) as count, SUM(amount) as total 
            FROM parquet_sales 
            GROUP BY product
        """).collect()

print("\n6.3. SELECT product, COUNT(*), SUM(amount) FROM sales_parquet GROUP BY product")
groupby_metrics = measure_multiple_times(query_groupby, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {groupby_metrics['avg']:.2f} ms")
if use_hive_table:
    result = spark.sql(f"""
        SELECT product, COUNT(*) as count, SUM(amount) as total 
        FROM {HIVE_DATABASE}.sales_parquet 
        GROUP BY product
    """)
else:
    result = spark.sql("""
        SELECT product, COUNT(*) as count, SUM(amount) as total 
        FROM parquet_sales 
        GROUP BY product
    """)
result.show()

# 6.4. COUNT
def query_count():
    if use_hive_table:
        return spark.sql(f"SELECT COUNT(*) as total FROM {HIVE_DATABASE}.sales_parquet").collect()[0]['total']
    else:
        return spark.sql("SELECT COUNT(*) as total FROM parquet_sales").collect()[0]['total']

print("\n6.4. SELECT COUNT(*) FROM sales_parquet")
count_metrics = measure_multiple_times(query_count, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {count_metrics['avg']:.2f} ms")

# 6.5. Requête complexe avec agrégations multiples
def query_complex():
    if use_hive_table:
        return spark.sql(f"""
            SELECT 
                product,
                COUNT(*) as count,
                SUM(amount) as total,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM {HIVE_DATABASE}.sales_parquet
            GROUP BY product
            ORDER BY total DESC
        """).collect()
    else:
        return spark.sql("""
            SELECT 
                product,
                COUNT(*) as count,
                SUM(amount) as total,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM parquet_sales
            GROUP BY product
            ORDER BY total DESC
        """).collect()

print("\n6.5. Requête complexe avec agrégations multiples")
complex_metrics = measure_multiple_times(query_complex, BENCHMARK_ITERATIONS)
print(f"  Temps moyen: {complex_metrics['avg']:.2f} ms")
if use_hive_table:
    result = spark.sql(f"""
        SELECT 
            product,
            COUNT(*) as count,
            SUM(amount) as total,
            AVG(amount) as avg_amount
        FROM {HIVE_DATABASE}.sales_parquet
        GROUP BY product
        ORDER BY total DESC
    """)
else:
    result = spark.sql("""
        SELECT 
            product,
            COUNT(*) as count,
            SUM(amount) as total,
            AVG(amount) as avg_amount
        FROM parquet_sales
        GROUP BY product
        ORDER BY total DESC
    """)
result.show()

# ============================================================
# 7. STOCKAGE DES MÉTRIQUES DANS HIVE
# ============================================================
print("\n" + "=" * 60)
print("7. STOCKAGE DES MÉTRIQUES DANS HIVE")
print("=" * 60)

# Préparer les métriques
metrics_data = [{
    "format": "parquet",
    "operation": "csv_read",
    "time_ms": csv_read_time,
    "time_min_ms": csv_read_time,
    "time_max_ms": csv_read_time,
    "time_avg_ms": csv_read_time,
    "rows_processed": row_count,
    "size_bytes": 0,
    "size_mb": 0.0,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "write",
    "time_ms": write_metrics['avg'],
    "time_min_ms": write_metrics['min'],
    "time_max_ms": write_metrics['max'],
    "time_avg_ms": write_metrics['avg'],
    "rows_processed": write_metrics['result'],
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "read",
    "time_ms": read_metrics['avg'],
    "time_min_ms": read_metrics['min'],
    "time_max_ms": read_metrics['max'],
    "time_avg_ms": read_metrics['avg'],
    "rows_processed": parquet_row_count,
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "sql_select_all",
    "time_ms": select_metrics['avg'],
    "time_min_ms": select_metrics['min'],
    "time_max_ms": select_metrics['max'],
    "time_avg_ms": select_metrics['avg'],
    "rows_processed": row_count,
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "sql_filter",
    "time_ms": filter_metrics['avg'],
    "time_min_ms": filter_metrics['min'],
    "time_max_ms": filter_metrics['max'],
    "time_avg_ms": filter_metrics['avg'],
    "rows_processed": filter_metrics['result'],
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "sql_groupby",
    "time_ms": groupby_metrics['avg'],
    "time_min_ms": groupby_metrics['min'],
    "time_max_ms": groupby_metrics['max'],
    "time_avg_ms": groupby_metrics['avg'],
    "rows_processed": len(groupby_metrics['result']),
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "sql_count",
    "time_ms": count_metrics['avg'],
    "time_min_ms": count_metrics['min'],
    "time_max_ms": count_metrics['max'],
    "time_avg_ms": count_metrics['avg'],
    "rows_processed": count_metrics['result'],
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}, {
    "format": "parquet",
    "operation": "sql_complex",
    "time_ms": complex_metrics['avg'],
    "time_min_ms": complex_metrics['min'],
    "time_max_ms": complex_metrics['max'],
    "time_avg_ms": complex_metrics['avg'],
    "rows_processed": len(complex_metrics['result']),
    "size_bytes": parquet_size_bytes,
    "size_mb": parquet_size_mb,
    "compression": "snappy",
    "timestamp": int(time.time())
}]

metrics_df = spark.createDataFrame(metrics_data)

# Créer ou remplacer la table (avec gestion d'erreur)
try:
    metrics_df.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE_PARQUET_METRICS}")
    print(f"✓ Métriques stockées dans {HIVE_DATABASE}.{HIVE_TABLE_PARQUET_METRICS}")
except Exception as e:
    print(f"⚠ Avertissement: Impossible de stocker dans Hive: {e}")
    print("  Sauvegarde locale uniquement...")
    # Sauvegarder en CSV localement comme alternative
    try:
        metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/tmp/parquet_metrics")
        print("  Métriques sauvegardées dans /tmp/parquet_metrics")
    except Exception as e2:
        print(f"  Erreur lors de la sauvegarde CSV: {e2}")

metrics_df.show(truncate=False)

# ============================================================
# RÉSUMÉ
# ============================================================
print("\n" + "=" * 60)
print("RÉSUMÉ DU BENCHMARK PARQUET")
print("=" * 60)
print(f"Lignes traitées: {row_count}")
print(f"Taille Parquet: {parquet_size_mb:.4f} MB ({parquet_size_bytes} bytes)")
print(f"Nombre de fichiers: {parquet_file_count}")
print(f"\nTemps d'écriture (moyen): {write_metrics['avg']:.2f} ms")
print(f"Temps de lecture (moyen): {read_metrics['avg']:.2f} ms")
print(f"Temps requête SELECT (moyen): {select_metrics['avg']:.2f} ms")
print(f"Temps requête FILTER (moyen): {filter_metrics['avg']:.2f} ms")
print(f"Temps requête GROUP BY (moyen): {groupby_metrics['avg']:.2f} ms")
print(f"Temps requête COUNT (moyen): {count_metrics['avg']:.2f} ms")
print(f"Temps requête COMPLEX (moyen): {complex_metrics['avg']:.2f} ms")

spark.stop()
print("\n✓ Benchmark Parquet terminé")

