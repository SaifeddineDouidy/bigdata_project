"""
Configuration centralisée pour le benchmark Parquet vs HBase
Toutes les variables importantes sont définies ici
"""

# ============================================================
# CHEMINS HDFS
# ============================================================
CSV_HDFS_PATH = "hdfs://namenode:8020/user/data/sample_sales.csv"
PARQUET_HDFS_DIR = "hdfs://namenode:8020/user/output/parquet_sample"
PARQUET_HDFS_DIR_ALT = "hdfs://namenode:8020/user/hive/warehouse/sales_parquet"

# ============================================================
# CONFIGURATION HBASE
# ============================================================
HBASE_TABLE = "sales"
HBASE_COLUMN_FAMILY = "cf"
HBASE_ROWKEY_COLUMN = "id"
# Point all clients to the embedded ZooKeeper running in the hbase-master
# container, because the HBase Docker image always rewrites its own quorum
# to localhost:2181 and does not actually use the external zookeeper service.
HBASE_ZK_QUORUM = "hbase-master"
HBASE_ZK_PORT = "2181"
HBASE_MASTER_HOST = "hbase-master"
HBASE_MASTER_PORT = "16000"

# Catalog JSON pour Spark-HBase connector
HBASE_CATALOG = {
    "table": {
        "namespace": "default",
        "name": HBASE_TABLE
    },
    "rowkey": HBASE_ROWKEY_COLUMN,
    "columns": {
        "id": {"cf": "rowkey", "col": HBASE_ROWKEY_COLUMN, "type": "string"},
        "product": {"cf": HBASE_COLUMN_FAMILY, "col": "product", "type": "string"},
        "amount": {"cf": HBASE_COLUMN_FAMILY, "col": "amount", "type": "string"},
        "date": {"cf": HBASE_COLUMN_FAMILY, "col": "date", "type": "string"}
    }
}

# ============================================================
# CONFIGURATION SPARK
# ============================================================
SPARK_MASTER = "spark://spark-master-new:7077"
SPARK_APP_NAME_HBASE = "benchmark_hbase"
SPARK_APP_NAME_PARQUET = "benchmark_parquet"
SPARK_APP_NAME_COMPARISON = "benchmark_comparison"

# Packages Spark nécessaires
SPARK_PACKAGES = [
    "org.apache.hbase:hbase-spark:2.0.0-alpha4",
    "org.apache.hbase:hbase-client:2.1.3",
    "org.apache.hbase:hbase-common:2.1.3"
]

# ============================================================
# CONFIGURATION HIVE
# ============================================================
HIVE_DATABASE = "perf"
HIVE_TABLE_HBASE_METRICS = "hbase_metrics"
HIVE_TABLE_PARQUET_METRICS = "parquet_metrics"
HIVE_TABLE_COMPARISON = "comparison_results"

HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
HIVE_WAREHOUSE_DIR = "hdfs://namenode:8020/user/hive/warehouse"

# ============================================================
# CONFIGURATION DOCKER
# ============================================================
DOCKER_CONTAINER_SPARK = "spark-master-new"
DOCKER_CONTAINER_HBASE = "hbase-master"
DOCKER_CONTAINER_NAMENODE = "namenode"

# ============================================================
# CONFIGURATION BENCHMARK
# ============================================================
# Nombre d'itérations pour calculer min/max/moyen
BENCHMARK_ITERATIONS = 3

# Requêtes SQL de test
SQL_QUERIES = {
    "select_all": "SELECT * FROM {table}",
    "filter_amount": "SELECT * FROM {table} WHERE amount > 100",
    "groupby_product": "SELECT product, COUNT(*) as count, SUM(amount) as total FROM {table} GROUP BY product",
    "count_all": "SELECT COUNT(*) as total FROM {table}"
}

# ============================================================
# FICHIERS DE SORTIE
# ============================================================
OUTPUT_DIR = "./benchmark_results"
COMPARISON_CSV = f"{OUTPUT_DIR}/comparison_results.csv"
COMPARISON_REPORT = f"{OUTPUT_DIR}/benchmark_report.md"

