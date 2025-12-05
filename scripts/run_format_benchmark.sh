#!/bin/bash
# Script pour orchestrer le benchmark multi-format (CSV, Parquet, ORC, Avro)
# Usage: ./scripts/run_format_benchmark.sh

set -e

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SPARK_CONTAINER="spark-master-new"
NAMENODE_CONTAINER="namenode"

# Fonction pour convertir le chemin Windows pour Docker
convert_path_for_docker() {
    local path="$1"
    if [[ "$path" == /c/* ]] || [[ "$path" == /d/* ]] || [[ "$path" == /[a-z]/* ]]; then
        local drive_letter="${path:1:1}"
        echo "${drive_letter^^}:${path:2}"
    else
        echo "$path"
    fi
}

echo -e "${GREEN}=== DÉMARRAGE DU BENCHMARK MULTI-FORMAT ===${NC}"

# 1. Préparation des scripts
echo -e "\n${YELLOW}1. Copie du script de benchmark dans le conteneur Spark...${NC}"
BENCHMARK_SCRIPT=$(convert_path_for_docker "$PROJECT_ROOT/benchmarks/benchmark_formats.py")
MSYS_NO_PATHCONV=1 docker cp "$BENCHMARK_SCRIPT" $SPARK_CONTAINER:/benchmark_formats.py

# 2. Benchmark Dataset "Small" (Existant)
echo -e "\n${YELLOW}2. Benchmark Dataset 'Small' (sample_sales.csv)...${NC}"
# S'assurer que le fichier est sur HDFS (via script existant ou manuel)
if ! MSYS_NO_PATHCONV=1 docker exec $NAMENODE_CONTAINER hdfs dfs -test -f /user/data/sample_sales.csv; then
    echo "Upload du fichier sample_sales.csv..."
    bash "$SCRIPT_DIR/upload_csv_to_hdfs.sh"
fi

echo "Exécution du benchmark Small..."
MSYS_NO_PATHCONV=1 docker exec $SPARK_CONTAINER /spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    --packages org.apache.spark:spark-avro_2.12:3.1.1 \
    /benchmark_formats.py \
    --input-file "hdfs://namenode:8020/user/data/sample_sales.csv" \
    --dataset-name "small"

# 3. Génération Dataset "Huge"
echo -e "\n${YELLOW}3. Génération Dataset 'Huge' (1M lignes)...${NC}"
HUGE_CSV="$PROJECT_ROOT/data/large_sales.csv"
if [ ! -f "$HUGE_CSV" ]; then
    python "$SCRIPT_DIR/generate_data.py" "$HUGE_CSV" 1000000
else
    echo "Fichier large_sales.csv déjà existant."
fi

# 4. Upload Dataset "Huge" sur HDFS
echo -e "\n${YELLOW}4. Upload Dataset 'Huge' sur HDFS...${NC}"
HUGE_CSV_DOCKER=$(convert_path_for_docker "$HUGE_CSV")
docker cp "$HUGE_CSV_DOCKER" $NAMENODE_CONTAINER:/tmp/large_sales.csv
MSYS_NO_PATHCONV=1 docker exec $NAMENODE_CONTAINER hdfs dfs -put -f /tmp/large_sales.csv /user/data/large_sales.csv

# 5. Benchmark Dataset "Huge"
echo -e "\n${YELLOW}5. Benchmark Dataset 'Huge'...${NC}"
echo "Exécution du benchmark Huge (cela peut prendre du temps)..."
MSYS_NO_PATHCONV=1 docker exec $SPARK_CONTAINER /spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    --packages org.apache.spark:spark-avro_2.12:3.1.1 \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    /benchmark_formats.py \
    --input-file "hdfs://namenode:8020/user/data/large_sales.csv" \
    --dataset-name "huge"

# 6. Affichage des résultats
echo -e "\n${GREEN}=== RÉSULTATS DU BENCHMARK ===${NC}"
echo "Récupération des résultats depuis Hive..."
MSYS_NO_PATHCONV=1 docker exec $SPARK_CONTAINER /spark/bin/spark-sql -e "SELECT * FROM perf.format_benchmark_results ORDER BY dataset, format;"

echo -e "\n${GREEN}Terminé!${NC}"
