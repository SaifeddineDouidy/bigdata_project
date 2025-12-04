#!/bin/bash
# Script principal orchestrant toutes les étapes du benchmark
# Usage: ./run_full_benchmark.sh

# Ne pas arrêter sur les erreurs pour continuer la vérification
set +e

echo "=========================================="
echo "BENCHMARK COMPLET: PARQUET vs HBASE"
echo "=========================================="
echo ""

# Couleurs
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_CONTAINER="spark-master-new"
HBASE_CONTAINER="hbase-master"

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

# Fonction pour exécuter une étape
run_step() {
    echo ""
    echo -e "${YELLOW}==========================================${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}==========================================${NC}"
    echo ""
}

# Fonction pour vérifier le succès
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
        return 0
    else
        echo -e "${RED}✗ $1${NC}"
        return 1
    fi
}

# ============================================================
# ÉTAPE 1: VÉRIFICATION DES SERVICES
# ============================================================
run_step "ÉTAPE 1: VÉRIFICATION DES SERVICES"

if [ -f "$SCRIPT_DIR/scripts/verify_services.sh" ]; then
    bash "$SCRIPT_DIR/scripts/verify_services.sh"
    if ! check_success "Vérification des services"; then
        echo -e "${YELLOW}⚠ Certains services ont des problèmes, mais continuons...${NC}"
    fi
else
    echo "Script de vérification non trouvé, continuation..."
fi

# ============================================================
# ÉTAPE 2: PRÉPARATION DE HBASE
# ============================================================
run_step "ÉTAPE 2: PRÉPARATION DE HBASE"

if [ -f "$SCRIPT_DIR/scripts/prepare_hbase.sh" ]; then
    bash "$SCRIPT_DIR/scripts/prepare_hbase.sh"
    if ! check_success "Préparation de HBase"; then
        echo -e "${YELLOW}⚠ Préparation HBase a des problèmes, mais continuons...${NC}"
    fi
else
    echo "Script de préparation HBase non trouvé, création manuelle..."
    docker exec $HBASE_CONTAINER bash -c "echo 'disable \"sales\"' | hbase shell -n" 2>/dev/null || true
    docker exec $HBASE_CONTAINER bash -c "echo 'drop \"sales\"' | hbase shell -n" 2>/dev/null || true
    docker exec $HBASE_CONTAINER bash -c "echo 'create \"sales\", \"cf\"' | hbase shell -n"
fi

# ============================================================
# ÉTAPE 3: CRÉATION DU CATALOG JSON (OPTIONNEL)
# ============================================================
run_step "ÉTAPE 3: CRÉATION DU CATALOG JSON (OPTIONNEL)"

echo "Note: Cette etape est optionnelle. Le catalog est defini dans les scripts de benchmark."

if [ -f "$SCRIPT_DIR/scripts/create_hbase_catalog.py" ]; then
    if command -v python3 &> /dev/null; then
        python3 "$SCRIPT_DIR/scripts/create_hbase_catalog.py" 2>/dev/null && \
            echo -e "${GREEN}✓ Catalog JSON créé${NC}" || \
            echo -e "${YELLOW}⚠ Création du catalog ignorée (optionnel)${NC}"
    elif command -v python &> /dev/null; then
        python "$SCRIPT_DIR/scripts/create_hbase_catalog.py" 2>/dev/null && \
            echo -e "${GREEN}✓ Catalog JSON créé${NC}" || \
            echo -e "${YELLOW}⚠ Création du catalog ignorée (optionnel)${NC}"
    else
        echo -e "${YELLOW}⚠ Python non trouvé, catalog ignoré (optionnel)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Script de création du catalog non trouvé (optionnel)${NC}"
fi

# ============================================================
# ÉTAPE 4: BENCHMARK HBASE
# ============================================================
run_step "ÉTAPE 4: BENCHMARK HBASE"

echo "Copie des scripts dans le conteneur Spark..."

# Convertir les chemins pour Windows
HBASE_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_hbase.py")
CONFIG_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_config.py")

echo "Fichier source HBase: $HBASE_SCRIPT"
echo "Fichier config: $CONFIG_SCRIPT"

# Copier avec MSYS_NO_PATHCONV pour éviter la conversion automatique
MSYS_NO_PATHCONV=1 docker cp "$HBASE_SCRIPT" $SPARK_CONTAINER:/benchmark_hbase.py
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Erreur lors de la copie de benchmark_hbase.py${NC}"
    exit 1
fi

MSYS_NO_PATHCONV=1 docker cp "$CONFIG_SCRIPT" $SPARK_CONTAINER:/benchmark_config.py 2>/dev/null || \
    echo "benchmark_config.py non trouvé, utilisation des valeurs par défaut"

# Vérifier que les fichiers sont bien copiés
echo "Vérification des fichiers copiés..."
docker exec $SPARK_CONTAINER ls -la /benchmark_hbase.py || {
    echo -e "${RED}✗ Le fichier benchmark_hbase.py n'existe pas dans le conteneur${NC}"
    exit 1
}

echo ""
echo "Exécution du benchmark HBase..."
echo "Cela peut prendre plusieurs minutes..."
docker exec $SPARK_CONTAINER /spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    --packages org.apache.hbase:hbase-client:2.1.3,org.apache.hbase:hbase-common:2.1.3 \
    /benchmark_hbase.py

if check_success "Benchmark HBase"; then
    echo "✓ Métriques HBase enregistrées dans perf.hbase_metrics"
else
    echo -e "${RED}Le benchmark HBase a échoué. Vérifiez les logs ci-dessus.${NC}"
    exit 1
fi

# ============================================================
# ÉTAPE 5: BENCHMARK PARQUET
# ============================================================
run_step "ÉTAPE 5: BENCHMARK PARQUET"

echo "Copie des scripts dans le conteneur Spark..."

# Convertir les chemins pour Windows
PARQUET_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_parquet_complete.py")
CONFIG_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_config.py")

echo "Fichier source Parquet: $PARQUET_SCRIPT"

MSYS_NO_PATHCONV=1 docker cp "$PARQUET_SCRIPT" $SPARK_CONTAINER:/benchmark_parquet_complete.py
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Erreur lors de la copie de benchmark_parquet_complete.py${NC}"
    exit 1
fi

MSYS_NO_PATHCONV=1 docker cp "$CONFIG_SCRIPT" $SPARK_CONTAINER:/benchmark_config.py 2>/dev/null || \
    echo "benchmark_config.py non trouvé, utilisation des valeurs par défaut"

# Vérifier que les fichiers sont bien copiés
docker exec $SPARK_CONTAINER ls -la /benchmark_parquet_complete.py || {
    echo -e "${RED}✗ Le fichier benchmark_parquet_complete.py n'existe pas dans le conteneur${NC}"
    exit 1
}

echo ""
echo "Exécution du benchmark Parquet..."
echo "Cela peut prendre plusieurs minutes..."
docker exec $SPARK_CONTAINER /spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    /benchmark_parquet_complete.py

if check_success "Benchmark Parquet"; then
    echo "✓ Métriques Parquet enregistrées dans perf.parquet_metrics"
else
    echo -e "${RED}Le benchmark Parquet a échoué. Vérifiez les logs ci-dessus.${NC}"
    exit 1
fi

# ============================================================
# ÉTAPE 6: GÉNÉRATION DU TABLEAU COMPARATIF
# ============================================================
run_step "ÉTAPE 6: GÉNÉRATION DU TABLEAU COMPARATIF"

echo "Copie des scripts dans le conteneur Spark..."

# Convertir les chemins pour Windows
COMPARISON_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/generate_comparison.py")
CONFIG_SCRIPT=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_config.py")

echo "Fichier source comparison: $COMPARISON_SCRIPT"

MSYS_NO_PATHCONV=1 docker cp "$COMPARISON_SCRIPT" $SPARK_CONTAINER:/generate_comparison.py
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Erreur lors de la copie de generate_comparison.py${NC}"
    exit 1
fi

MSYS_NO_PATHCONV=1 docker cp "$CONFIG_SCRIPT" $SPARK_CONTAINER:/benchmark_config.py 2>/dev/null || \
    echo "benchmark_config.py non trouvé, utilisation des valeurs par défaut"

# Vérifier que les fichiers sont bien copiés
docker exec $SPARK_CONTAINER ls -la /generate_comparison.py || {
    echo -e "${RED}✗ Le fichier generate_comparison.py n'existe pas dans le conteneur${NC}"
    exit 1
}

echo ""
echo "Génération du tableau comparatif..."
docker exec $SPARK_CONTAINER /spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    /generate_comparison.py

if check_success "Génération du tableau comparatif"; then
    echo "✓ Résultats comparatifs disponibles"
else
    echo -e "${YELLOW}⚠ La génération du comparatif a échoué${NC}"
    echo "Vous pouvez consulter les métriques individuelles dans Hive"
fi

# ============================================================
# COPIE DES RÉSULTATS DEPUIS LE CONTENEUR
# ============================================================
echo ""
echo "Tentative de récupération des résultats depuis le conteneur..."

# Créer le répertoire local
mkdir -p "$SCRIPT_DIR/benchmark_results"

# Essayer de copier les résultats si disponibles
docker exec $SPARK_CONTAINER test -d /tmp/benchmark_results 2>/dev/null
if [ $? -eq 0 ]; then
    RESULTS_DIR=$(convert_path_for_docker "$SCRIPT_DIR/benchmark_results")
    MSYS_NO_PATHCONV=1 docker cp $SPARK_CONTAINER:/tmp/benchmark_results/. "$RESULTS_DIR/" 2>/dev/null && \
        echo "✓ Résultats copiés localement" || \
        echo "⚠ Impossible de copier les résultats localement"
else
    echo "⚠ Pas de résultats à copier (les résultats sont dans Hive)"
fi

# ============================================================
# RÉSUMÉ FINAL
# ============================================================
echo ""
echo "=========================================="
echo -e "${GREEN}BENCHMARK COMPLET TERMINÉ${NC}"
echo "=========================================="
echo ""
echo "Résultats disponibles:"
echo "  - Table Hive HBase: perf.hbase_metrics"
echo "  - Table Hive Parquet: perf.parquet_metrics"
echo "  - Table Hive Comparaison: perf.comparison_results"
echo ""
echo "Fichiers locaux (si disponibles):"
echo "  - CSV: benchmark_results/comparison_results.csv"
echo "  - Rapport: benchmark_results/benchmark_report.md"
echo ""
echo "Pour consulter les résultats dans Spark SQL:"
echo "  docker exec -it $SPARK_CONTAINER /spark/bin/spark-sql"
echo "  > USE perf;"
echo "  > SELECT * FROM hbase_metrics;"
echo "  > SELECT * FROM parquet_metrics;"
echo "  > SELECT * FROM comparison_results;"
echo ""
echo "Vérifier les fichiers Parquet sur HDFS:"
echo "  docker exec namenode hdfs dfs -ls /user/output/parquet_sample"
echo ""