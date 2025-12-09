#!/usr/bin/env bash
set -e

# Usage :
#   ./scripts/run_spark_ml.sh
# ou :
#   ./scripts/run_spark_ml.sh autre-container autre.table

CONTAINER_NAME=${1:-spark-master-new}
HIVE_TABLE=${2:-perf.sales_parquet}

echo ">>> Running Spark ML benchmark in container '$CONTAINER_NAME' (table = $HIVE_TABLE)..."

# MSYS_NO_PATHCONV pour éviter les problèmes de chemins sous Git Bash (Windows)
MSYS_NO_PATHCONV=1 docker exec -i "$CONTAINER_NAME" bash << EOF
set -e

echo "=== [1/3] Spark ML benchmark (ml_benchmark.py) ==="
cd /spark_ml

PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
  /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  ml_benchmark.py \
  --table $HIVE_TABLE

echo "=== [2/3] Génération du script Python d'export CSV ==="
cat > /tmp/export_ml_results.py << 'PY'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ExportMLResults")
    .enableHiveSupport()
    .getOrCreate()
)

# Lire la table Hive des résultats ML
df = spark.table("perf.ml_benchmark_results")

output_dir = "/spark_ml/output_ml_results"

# Écriture en CSV avec header, dans un seul fichier (coalesce(1))
(df
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", True)
 .csv(output_dir)
)

print(f"✓ CSV export written to {output_dir}")
spark.stop()
PY

echo "=== [3/3] Exécution de l'export CSV avec spark-submit ==="
/spark/bin/spark-submit /tmp/export_ml_results.py
rm -f /tmp/export_ml_results.py

echo ">>> Terminé. Les fichiers CSV se trouvent dans /spark_ml/output_ml_results (dans le conteneur)."
EOF
