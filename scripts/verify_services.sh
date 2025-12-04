#!/bin/bash
# Script de vérification des services Docker/HBase/HDFS
# Usage: ./verify_services.sh

# Ne pas arrêter sur les erreurs pour continuer la vérification
set +e

echo "=========================================="
echo "VÉRIFICATION DES SERVICES"
echo "=========================================="

# Couleurs pour l'affichage
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Fonction pour afficher le statut
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

# 1. Vérifier Docker
echo ""
echo "1. Vérification Docker..."
if command -v docker &> /dev/null; then
    print_status 0 "Docker installé"
    
    # Vérifier que Docker est en cours d'exécution
    if docker ps &> /dev/null; then
        print_status 0 "Docker daemon actif"
        echo ""
        echo "Conteneurs en cours d'exécution:"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    else
        print_status 1 "Docker daemon non actif"
        exit 1
    fi
else
    print_status 1 "Docker non installé"
    exit 1
fi

# 2. Vérifier les conteneurs spécifiques
echo ""
echo "2. Vérification des conteneurs requis..."

CONTAINERS=("namenode" "datanode" "zookeeper" "hbase-master" "hbase-regionserver" "spark-master-new" "spark-worker" "hive-metastore")

for container in "${CONTAINERS[@]}"; do
    if docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
        STATUS=$(docker inspect --format='{{.State.Status}}' $container 2>/dev/null)
        if [ "$STATUS" == "running" ]; then
            print_status 0 "$container (running)"
        else
            print_status 1 "$container ($STATUS)"
        fi
    else
        print_status 1 "$container (non trouvé)"
    fi
done

# 3. Vérifier HDFS
echo ""
echo "3. Vérification HDFS..."
if docker exec namenode hdfs dfs -ls / >/dev/null 2>&1; then
    print_status 0 "HDFS accessible"
    echo ""
    echo "Contenu de la racine HDFS:"
    docker exec namenode hdfs dfs -ls / 2>&1 | head -10
else
    print_status 1 "HDFS non accessible"
    echo "Tentative de diagnostic..."
    docker logs namenode --tail 20 2>&1 | tail -5
fi

# 4. Vérifier HBase
echo ""
echo "4. Vérification HBase..."
HBASE_STATUS=$(docker exec hbase-master bash -c "echo 'status' | hbase shell -n" 2>&1)
if echo "$HBASE_STATUS" | grep -q "active master"; then
    print_status 0 "HBase shell accessible"
    echo ""
    echo "Tables HBase existantes:"
    HBASE_LIST=$(docker exec hbase-master bash -c "echo 'list' | hbase shell -n" 2>&1)
    echo "$HBASE_LIST" | grep -A 20 "TABLE" || echo "Aucune table trouvée"
else
    print_status 1 "HBase shell non accessible"
    echo "Tentative de diagnostic..."
    docker logs hbase-master --tail 20 2>&1 | tail -5
fi

# 5. Vérifier HBase Master et RegionServer
echo ""
echo "5. Vérification HBase Master/RegionServer..."
HM_CHECK=$(docker exec hbase-master bash -c "ps aux | grep -E 'HMaster' | grep -v grep" 2>&1)
if [ -n "$HM_CHECK" ]; then
    print_status 0 "HMaster en cours d'exécution"
else
    print_status 1 "HMaster non trouvé"
fi

HRS_CHECK=$(docker exec hbase-regionserver bash -c "ps aux | grep -E 'HRegionServer' | grep -v grep" 2>&1)
if [ -n "$HRS_CHECK" ]; then
    print_status 0 "HRegionServer en cours d'exécution"
else
    # Vérifier aussi dans hbase-master (parfois les deux sont dans le même conteneur)
    HRS_CHECK2=$(docker exec hbase-master bash -c "ps aux | grep -E 'HRegionServer' | grep -v grep" 2>&1)
    if [ -n "$HRS_CHECK2" ]; then
        print_status 0 "HRegionServer en cours d'exécution (dans hbase-master)"
    else
        print_status 1 "HRegionServer non trouvé"
    fi
fi

# 6. Vérifier Zookeeper
echo ""
echo "6. Vérification Zookeeper..."
ZK_CHECK=$(docker exec zookeeper bash -c "echo ruok | nc localhost 2181" 2>&1)
if echo "$ZK_CHECK" | grep -q "imok"; then
    print_status 0 "Zookeeper répond (imok)"
elif echo "$ZK_CHECK" | grep -q "whitelist"; then
    # Zookeeper répond mais la commande n'est pas dans la whitelist - c'est OK, il fonctionne
    print_status 0 "Zookeeper accessible (commande ruok non autorisée mais service actif)"
else
    # Vérifier que le conteneur est en cours d'exécution
    if docker ps --format "{{.Names}}" | grep -q "^zookeeper$"; then
        print_status 0 "Zookeeper conteneur en cours d'exécution"
    else
        print_status 1 "Zookeeper ne répond pas"
    fi
fi

# 7. Vérifier Spark
echo ""
echo "7. Vérification Spark..."
SPARK_MASTER_CHECK=$(docker exec spark-master-new bash -c "ps aux | grep -E 'Master' | grep -v grep" 2>&1)
if [ -n "$SPARK_MASTER_CHECK" ]; then
    print_status 0 "Spark Master en cours d'exécution"
else
    print_status 1 "Spark Master non trouvé"
fi

SPARK_WORKER_CHECK=$(docker exec spark-worker bash -c "ps aux | grep -E 'Worker' | grep -v grep" 2>&1)
if [ -n "$SPARK_WORKER_CHECK" ]; then
    print_status 0 "Spark Worker en cours d'exécution"
else
    print_status 1 "Spark Worker non trouvé"
fi

# 8. Vérifier Hive Metastore
echo ""
echo "8. Vérification Hive Metastore..."
if docker exec hive-metastore nc -z localhost 9083 2>/dev/null; then
    print_status 0 "Hive Metastore accessible (port 9083)"
else
    print_status 1 "Hive Metastore non accessible"
fi

# 9. Vérifier le fichier CSV sur HDFS
echo ""
echo "9. Vérification du dataset CSV sur HDFS..."
CSV_CHECK=$(docker exec namenode hdfs dfs -test -f /user/data/sample_sales.csv 2>&1)
CSV_EXIT=$?
if [ $CSV_EXIT -eq 0 ]; then
    print_status 0 "Fichier CSV trouvé sur HDFS"
    echo ""
    echo "Taille du fichier:"
    docker exec namenode hdfs dfs -du -h /user/data/sample_sales.csv 2>&1
    echo ""
    echo "Premières lignes:"
    docker exec namenode hdfs dfs -cat /user/data/sample_sales.csv 2>&1 | head -5
else
    print_status 1 "Fichier CSV non trouvé sur HDFS"
    echo "Le fichier doit être uploadé sur HDFS avant de continuer"
    echo "Commande: bash scripts/upload_csv_to_hdfs.sh"
fi

echo ""
echo "=========================================="
echo "VÉRIFICATION TERMINÉE"
echo "=========================================="

