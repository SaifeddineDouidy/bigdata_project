#!/bin/bash
# Script pour uploader le CSV sur HDFS
# Usage: ./upload_csv_to_hdfs.sh

set -e

echo "=========================================="
echo "UPLOAD DU CSV SUR HDFS"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)/.."
CSV_FILE="$SCRIPT_DIR/data/large_sales.csv"
HDFS_PATH="/user/data/large_sales.csv"
CONTAINER="namenode"

# Vérifier que le fichier existe
if [ ! -f "$CSV_FILE" ]; then
    echo "ERREUR: Fichier $CSV_FILE non trouvé"
    exit 1
fi

# Convertir le chemin Windows pour docker cp
# Git Bash convertit /c/Users/... en C:/Users/...
if [[ "$CSV_FILE" == /c/* ]] || [[ "$CSV_FILE" == /d/* ]] || [[ "$CSV_FILE" == /[a-z]/* ]]; then
    # Convertir /c/Users/... en C:/Users/...
    DRIVE_LETTER="${CSV_FILE:1:1}"
    CSV_FILE_DOCKER="${DRIVE_LETTER^^}:${CSV_FILE:2}"
else
    CSV_FILE_DOCKER="$CSV_FILE"
fi

echo "Fichier source: $CSV_FILE"
echo "Chemin Docker: $CSV_FILE_DOCKER"
echo "Destination HDFS: $HDFS_PATH"

# Créer le répertoire sur HDFS si nécessaire
echo ""
echo "Création du répertoire /user/data sur HDFS..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -mkdir -p /user/data
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -chmod 777 /user/data

# Copier le fichier dans le conteneur
echo ""
echo "Copie du fichier dans le conteneur..."
docker cp "$CSV_FILE_DOCKER" $CONTAINER:/tmp/large_sales.csv

# Uploader sur HDFS
echo ""
echo "Upload du fichier sur HDFS..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -put -f /tmp/large_sales.csv $HDFS_PATH

# Vérifier
echo ""
echo "Vérification de l'upload..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -ls $HDFS_PATH
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -cat $HDFS_PATH | head -5

echo ""
echo "=========================================="
echo "✓ CSV uploadé avec succès sur HDFS"
echo "=========================================="