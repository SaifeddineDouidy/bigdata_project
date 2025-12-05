# GUIDE COMPLET DU BENCHMARK PARQUET vs HBASE

Ce guide explique comment exécuter le benchmark complet comparant les performances de Parquet et HBase avec Spark SQL.

## 📋 PRÉREQUIS

1. **Docker Desktop** installé et en cours d'exécution
2. **Tous les services** démarrés via `docker-compose up -d`
3. **Fichier CSV** uploadé sur HDFS

## 🚀 DÉMARRAGE RAPIDE

### Option 1: Script automatique (recommandé)

```bash
# Rendre le script exécutable
chmod +x run_full_benchmark.sh

# Exécuter le benchmark complet
./run_full_benchmark.sh
```

### Option 2: Exécution manuelle étape par étape

Suivez les étapes ci-dessous.

---

## 📝 ÉTAPES DÉTAILLÉES

### ÉTAPE 1: Vérification des services

Vérifiez que tous les services sont opérationnels:

```bash
bash scripts/verify_services.sh
```

Ou manuellement:

```bash
# Vérifier Docker
docker ps

# Vérifier HDFS
docker exec namenode hdfs dfs -ls /

# Vérifier HBase
docker exec hbase-master bash -c "echo 'list' | hbase shell -n"

# Vérifier Spark
docker exec spark-master-new jps
```

### ÉTAPE 2: Upload du CSV sur HDFS

Si le fichier CSV n'est pas déjà sur HDFS:

```bash
bash scripts/upload_csv_to_hdfs.sh
```

Ou manuellement:

```bash
docker cp sample_sales.csv namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -put /tmp/sample_sales.csv /user/data/sample_sales.csv
```

### ÉTAPE 3: Préparation de HBase

Créez la table HBase:

```bash
bash scripts/prepare_hbase.sh
```

Ou manuellement:

```bash
docker exec hbase-master bash -c "echo 'create \"sales\", \"cf\"' | hbase shell -n"
docker exec hbase-master bash -c "echo 'list' | hbase shell -n"
```

### ÉTAPE 4: Benchmark HBase

Exécutez le benchmark HBase:

```bash
# Copier les scripts dans le conteneur Spark
docker cp benchmark_hbase.py spark-master-new:/benchmark_hbase.py
docker cp benchmark_config.py spark-master-new:/benchmark_config.py

# Exécuter le benchmark
docker exec spark-master-new /opt/spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    --packages org.apache.hbase:hbase-client:2.1.3,org.apache.hbase:hbase-common:2.1.3 \
    /benchmark_hbase.py
```

### ÉTAPE 5: Benchmark Parquet

Exécutez le benchmark Parquet:

```bash
# Copier les scripts dans le conteneur Spark
docker cp benchmark_parquet_complete.py spark-master-new:/benchmark_parquet_complete.py
docker cp benchmark_config.py spark-master-new:/benchmark_config.py

# Exécuter le benchmark
docker exec spark-master-new /opt/spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    /benchmark_parquet_complete.py
```

### ÉTAPE 6: Génération du tableau comparatif

Générez le tableau comparatif final:

```bash
# Copier les scripts dans le conteneur Spark
docker cp generate_comparison.py spark-master-new:/generate_comparison.py
docker cp benchmark_config.py spark-master-new:/benchmark_config.py

# Exécuter la génération
docker exec spark-master-new /opt/spark/bin/spark-submit \
    --master spark://spark-master-new:7077 \
    /generate_comparison.py
```

---

## 📊 CONSULTATION DES RÉSULTATS

### Via Spark SQL

```bash
docker exec -it spark-master-new /opt/spark/bin/spark-sql
```

Puis dans Spark SQL:

```sql
USE perf;

-- Voir les métriques HBase
SELECT * FROM hbase_metrics;

-- Voir les métriques Parquet
SELECT * FROM parquet_metrics;

-- Voir le tableau comparatif
SELECT * FROM comparison_results;
```

### Via fichiers locaux

Les résultats sont également disponibles dans:

- **CSV:** `benchmark_results/comparison_results.csv`
- **Rapport Markdown:** `benchmark_results/benchmark_report.md`

### Via HDFS

```bash
# Voir les fichiers Parquet
docker exec namenode hdfs dfs -ls /user/output/parquet_sample

# Voir la taille
docker exec namenode hdfs dfs -du -h /user/output/parquet_sample
```

---

## 🔧 CONFIGURATION

Toutes les variables de configuration sont dans `benchmark_config.py`:

- Chemins HDFS
- Noms de tables HBase
- Noms de tables Hive
- Nombre d'itérations pour les benchmarks

Modifiez ce fichier selon vos besoins.

---

## 📈 MÉTRIQUES MESURÉES

### Pour HBase:
- Temps de lecture CSV
- Temps d'écriture (min/max/moyen)
- Temps de scan complet
- Temps de conversion en DataFrame
- Temps des requêtes Spark SQL (SELECT, FILTER, GROUP BY, COUNT)

### Pour Parquet:
- Temps de lecture CSV
- Temps d'écriture (min/max/moyen)
- Temps de lecture
- Taille du dossier Parquet
- Compression utilisée
- Temps des requêtes Spark SQL (SELECT, FILTER, GROUP BY, COUNT, requêtes complexes)

### Comparaison:
- Speedup (ratio de performance)
- Différence de temps
- Différence en pourcentage

---

## 🐛 DÉPANNAGE

### Problème: HBase n'est pas accessible

```bash
# Vérifier les logs
docker logs hbase-master
docker logs hbase-regionserver
docker logs zookeeper

# Vérifier l'état
docker exec hbase-master jps
docker exec hbase-regionserver jps
```

### Problème: Spark ne peut pas se connecter à HDFS

```bash
# Vérifier la configuration
docker exec spark-master-new cat /opt/hadoop/etc/hadoop/core-site.xml

# Tester la connexion
docker exec spark-master-new hdfs dfs -ls /
```

### Problème: Les tables Hive n'existent pas

```bash
# Vérifier la connexion au metastore
docker logs hive-metastore

# Vérifier PostgreSQL
docker exec hive-metastore-postgresql psql -U hiveuser -d metastore -c "\dt"
```

### Problème: Erreur de dépendances HBase dans Spark

Les packages HBase doivent être téléchargés lors du spark-submit. Si cela échoue:

1. Vérifiez la connectivité réseau du conteneur
2. Utilisez `--repositories` si nécessaire
3. Vérifiez que les versions sont compatibles

---

## 📚 STRUCTURE DES FICHIERS

```
bigdata_project/
├── benchmark_config.py              # Configuration centralisée
├── benchmark_hbase.py               # Script benchmark HBase
├── benchmark_parquet_complete.py    # Script benchmark Parquet
├── generate_comparison.py           # Script de comparaison
├── run_full_benchmark.sh            # Script principal
├── scripts/
│   ├── verify_services.sh           # Vérification des services
│   ├── prepare_hbase.sh             # Préparation HBase
│   ├── upload_csv_to_hdfs.sh        # Upload CSV
│   └── create_hbase_catalog.py      # Génération catalog JSON
├── benchmark_results/               # Résultats (généré)
│   ├── comparison_results.csv
│   └── benchmark_report.md
└── ANALYSE_PROJET.md                # Analyse initiale
```

---

## 🎯 PROCHAINES ÉTAPES

Après avoir exécuté le benchmark:

1. **Analyser les résultats** dans le rapport markdown
2. **Comparer les performances** selon vos cas d'usage
3. **Ajuster la configuration** si nécessaire
4. **Tester avec un dataset plus volumineux** pour des résultats plus représentatifs

---

## 📞 SUPPORT

En cas de problème:

1. Consultez les logs des conteneurs: `docker logs <container>`
2. Vérifiez l'état des services: `bash scripts/verify_services.sh`
3. Consultez `ANALYSE_PROJET.md` pour l'analyse initiale

---

**Bon benchmark! 🚀**

