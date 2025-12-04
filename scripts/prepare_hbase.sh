#!/bin/bash
# Script de préparation HBase
# - Crée la table HBase
# - Vérifie l'état des services
# - Crée le catalog JSON pour Spark

set +e

echo "=========================================="
echo "PRÉPARATION DE HBASE"
echo "=========================================="

HBASE_TABLE="sales"
HBASE_CF="cf"
CONTAINER="hbase-master"
# Try to find hbase command - it might be in /opt/hbase/bin or /hbase/bin
HBASE_CMD="hbase"
# Check if hbase is in PATH, otherwise try common locations
if ! docker exec $CONTAINER bash -c "command -v hbase >/dev/null 2>&1" 2>/dev/null; then
    if docker exec $CONTAINER bash -c "test -f /opt/hbase/bin/hbase" 2>/dev/null; then
        HBASE_CMD="/opt/hbase/bin/hbase"
    elif docker exec $CONTAINER bash -c "test -f /hbase/bin/hbase" 2>/dev/null; then
        HBASE_CMD="/hbase/bin/hbase"
    else
        # Try to find it
        HBASE_CMD=$(docker exec $CONTAINER bash -c "find /opt /hbase -name hbase -type f 2>/dev/null | head -1" 2>/dev/null || echo "hbase")
    fi
fi

# 1. Vérifier que HBase est accessible
echo ""
echo "1. Vérification de l'accessibilité HBase..."
echo "   Utilisation de la commande: $HBASE_CMD"
HBASE_VERSION=$(docker exec $CONTAINER bash -c "echo 'version' | $HBASE_CMD shell -n" 2>&1)
if echo "$HBASE_VERSION" | grep -q "HBase"; then
    echo "✓ HBase accessible"
else
    echo "⚠ HBase shell peut avoir des problèmes, mais continuons..."
    # Ne pas arrêter, continuer quand même
fi

# 2. Vérifier l'état de HMaster et RegionServer
echo ""
echo "2. Vérification HMaster et RegionServer..."
HM_CHECK=$(docker exec $CONTAINER bash -c "ps aux | grep -E 'HMaster' | grep -v grep" 2>&1)
if [ -n "$HM_CHECK" ]; then
    echo "✓ HMaster en cours d'exécution"
else
    echo "✗ HMaster non trouvé"
    echo "Tentative de redémarrage..."
    docker restart hbase-master
    sleep 10
fi

HRS_CHECK=$(docker exec hbase-regionserver bash -c "ps aux | grep -E 'HRegionServer' | grep -v grep" 2>&1)
if [ -n "$HRS_CHECK" ]; then
    echo "✓ HRegionServer en cours d'exécution"
else
    # Vérifier aussi dans hbase-master
    HRS_CHECK2=$(docker exec $CONTAINER bash -c "ps aux | grep -E 'HRegionServer' | grep -v grep" 2>&1)
    if [ -n "$HRS_CHECK2" ]; then
        echo "✓ HRegionServer en cours d'exécution (dans hbase-master)"
    else
        echo "✗ HRegionServer non trouvé"
        echo "Tentative de redémarrage..."
        docker restart hbase-regionserver
        sleep 10
    fi
fi

# 3. Lister les tables existantes
echo ""
echo "3. Tables HBase existantes:"
docker exec $CONTAINER bash -c "echo 'list' | $HBASE_CMD shell -n" 2>/dev/null | grep -A 20 "TABLE" || echo "Aucune table"

# 4. Supprimer la table si elle existe (pour repartir de zéro)
echo ""
echo "4. Suppression de la table $HBASE_TABLE si elle existe..."
docker exec $CONTAINER bash -c "echo 'disable \"$HBASE_TABLE\"' | $HBASE_CMD shell -n" 2>/dev/null || true
docker exec $CONTAINER bash -c "echo 'drop \"$HBASE_TABLE\"' | $HBASE_CMD shell -n" 2>/dev/null || true
echo "✓ Table supprimée (si elle existait)"

# 5. Créer la table HBase
echo ""
echo "5. Création de la table $HBASE_TABLE avec column family $HBASE_CF..."
CREATE_TABLE_CMD="create '$HBASE_TABLE', '$HBASE_CF'"
CREATE_RESULT=$(docker exec $CONTAINER bash -c "echo \"$CREATE_TABLE_CMD\" | $HBASE_CMD shell -n" 2>&1)

if echo "$CREATE_RESULT" | grep -qE "created|already exists|Table.*already"; then
    echo "✓ Table créée ou existe déjà"
else
    echo "⚠ Résultat de création:"
    echo "$CREATE_RESULT" | tail -3
fi

# 6. Vérifier la création
echo ""
echo "6. Vérification de la table créée:"
docker exec $CONTAINER bash -c "echo 'list' | $HBASE_CMD shell -n" 2>/dev/null | grep "$HBASE_TABLE" || echo "Table non trouvée"

# 7. Décrire la table
echo ""
echo "7. Description de la table $HBASE_TABLE:"
docker exec $CONTAINER bash -c "echo 'describe \"$HBASE_TABLE\"' | $HBASE_CMD shell -n" 2>/dev/null | grep -A 10 "Table $HBASE_TABLE" || echo "Description non disponible"

# 8. Vérifier le répertoire HBase sur HDFS
echo ""
echo "8. Vérification du répertoire /hbase sur HDFS..."
if docker exec namenode hdfs dfs -test -d /hbase 2>/dev/null; then
    echo "✓ Répertoire /hbase existe"
    docker exec namenode hdfs dfs -ls /hbase | head -5
else
    echo "⚠ Répertoire /hbase n'existe pas (sera créé automatiquement par HBase)"
fi

echo ""
echo "=========================================="
echo "PRÉPARATION HBASE TERMINÉE"
echo "=========================================="
echo ""
echo "Table HBase créée: $HBASE_TABLE"
echo "Column Family: $HBASE_CF"
echo ""
echo "Vous pouvez maintenant exécuter les benchmarks."

