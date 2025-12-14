# 🚀 GUIDE COMPLET DES COMMANDES - Projet Big Data  
## Comparaison Avro, ORC, Parquet avec Hive, Spark SQL et Spark ML

Ce guide contient **TOUTES les commandes de A à Z** pour exécuter et tester le projet complet **depuis PowerShell sous Windows**.

> 💡 Si tu veux exécuter les scripts `.sh`, tu peux les lancer avec `bash` depuis PowerShell (Git Bash installé avec Git for Windows).

---

## 📋 Table des matières

1. [Prérequis et Démarrage](#1-prérequis-et-démarrage)  
2. [Vérification des Services](#2-vérification-des-services)  
3. [Génération et Upload des Données](#3-génération-et-upload-des-données)  
4. [Benchmark des Formats (CSV, Avro, ORC, Parquet)](#4-benchmark-des-formats-csv-avro-orc-parquet)  
5. [Lecture CSV avec Spark SQL + Hive](#5-lecture-csv-avec-spark-sql--hive)  
6. [Benchmark Parquet vs HBase](#6-benchmark-parquet-vs-hbase)  
7. [Spark ML - Benchmark des Modèles](#7-spark-ml---benchmark-des-modèles)  
8. [Consultation des Résultats](#8-consultation-des-résultats)  
9. [Commandes Utiles](#9-commandes-utiles)  
10. [Arrêt et Nettoyage](#10-arrêt-et-nettoyage)

---

## 1. Prérequis et Démarrage

### 1.1 Cloner le projet (si pas déjà fait)

```powershell
git clone <url-du-repo>
cd bigdata_project
```

### 1.2 Démarrer tous les services Docker

```powershell
# Démarrage de l'infrastructure complète
docker-compose up -d

# Vérifier l'état des conteneurs
docker-compose ps
```

### 1.3 Vérifier que tous les conteneurs sont "Up"

```powershell
docker ps --format "table {{.Names}}	{{.Status}}	{{.Ports}}"
```

**Services attendus :**

- `namenode` (port 9870)  
- `datanode` (port 9864)  
- `zookeeper` (port 2181)  
- `hive-metastore` (port 9083)  
- `hive-metastore-postgresql` (port 5432)  
- `spark-master-new` (ports 8080, 7077)  
- `spark-worker` (port 8081)  
- `hbase-master` (port 16010)  
- `hbase-regionserver` (port 16030)

---

## 2. Vérification des Services

### 2.1 Vérifier HDFS (NameNode)

```powershell
# Rapport HDFS
docker exec namenode hdfs dfsadmin -report

# Lister le contenu racine HDFS
docker exec namenode hdfs dfs -ls /
```

### 2.2 Vérifier Hive Metastore

```powershell
docker exec hive-metastore nc -zv hive-metastore 9083
```

### 2.3 Vérifier HBase (⚠️ version PowerShell)



#### Option B – One-liner (sans entrer dans le shell)

```powershell
docker exec hbase-master /bin/sh -lc "echo 'status' | /hbase/bin/hbase shell"

docker exec hbase-master /bin/sh -lc "echo 'list'   | /hbase/bin/hbase shell"

```

### 2.4 Vérifier Spark

```powershell
docker exec spark-master-new curl -s http://localhost:8080 | Select-Object -First 20
```

### 2.5 Script automatique de vérification (via bash)

```powershell
# Sous PowerShell, lance le script avec bash
./scripts/verify_services.sh
```

---

## 3. Génération et Upload des Données

### 3.1 Créer les répertoires HDFS

```powershell

docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -mkdir -p /user/output
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /user
```

### 3.2 Générer un petit dataset de test (`sample_sales.csv`)

En PowerShell, on utilise un **here-string** :

```powershell
@'
id,product,amount,date
1,Laptop,999.99,2023-01-15
2,Mouse,29.99,2023-02-20
3,Keyboard,79.99,2023-03-10
4,Monitor,299.99,2023-04-05
5,Headphones,149.99,2023-05-12
'@ | Set-Content -Encoding UTF8 .\data\sample_sales.csv

Write-Host "✓ Fichier sample_sales.csv créé"
```

### 3.3 Générer un grand dataset (1 million de lignes)

```powershell
.\scripts\generate_data.py .\data\large_sales.csv 1000000
```

Vérifier rapidement :

```powershell
# Info sur le fichier
Get-Item .\data\large_sales.csv | Select-Object Name, Length

# Nombre de lignes (optionnel, peut être un peu long)
Get-Content .\data\large_sales.csv | Measure-Object -Line
```

### 3.4 Uploader le petit dataset sur HDFS

```powershell
docker cp .\data\sample_sales.csv namenode:/tmp/sample_sales.csv
docker exec namenode hdfs dfs -put -f /tmp/sample_sales.csv /user/data/sample_sales.csv

docker exec namenode hdfs dfs -ls /user/data/
docker exec namenode hdfs dfs -cat /user/data/sample_sales.csv | Select-Object -First 10

```

### 3.5 Uploader le grand dataset sur HDFS

```powershell
docker cp .\data\large_sales.csv namenode:/tmp/large_sales.csv
docker exec namenode hdfs dfs -put -f /tmp/large_sales.csv /user/data/large_sales.csv

docker exec namenode hdfs dfs -ls /user/data/
docker exec namenode hdfs dfs -du -h /user/data/
```

---

## 4. Benchmark des Formats (CSV, Avro, ORC, Parquet)

### 4.1 Copier le script de benchmark dans Spark

```powershell
docker cp .\benchmarks\benchmark_formats.py spark-master-new:/benchmark_formats.py

```

### 4.2 Exécuter le benchmark sur le petit dataset

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  --packages org.apache.spark:spark-avro_2.12:3.1.1 `
  /benchmark_formats.py `
  --input-file "hdfs://namenode:8020/user/data/sample_sales.csv" `
  --dataset-name "small"
```

### 4.3 Exécuter le benchmark sur le grand dataset

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  --packages org.apache.spark:spark-avro_2.12:3.1.1 `
  --conf spark.driver.memory=2g `
  --conf spark.executor.memory=2g `
  /benchmark_formats.py `
  --input-file "hdfs://namenode:8020/user/data/large_sales.csv" `
  --dataset-name "huge"
```

### 4.4 Voir les résultats du benchmark des formats

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e `
"SELECT * FROM perf.format_benchmark_results ORDER BY dataset, format;"
```

### 4.5 Script automatisé complet (format benchmark)

```powershell
# Sous PowerShell, lance le script .sh avec bash
 ./scripts/run_format_benchmark.sh
```

---

## 5. Lecture CSV avec Spark SQL + Hive

### 5.1 Copier le script de lecture CSV

```powershell
docker cp .\benchmarks\read_csv_hive.py spark-master-new:/read_csv_hive.py

```

### 5.2 Lire le CSV et créer une table Hive

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  /read_csv_hive.py
```

### 5.3 Tester interactivement avec `spark-sql`

```powershell
docker exec -it spark-master-new /spark/bin/spark-sql
```

Dans le shell `spark-sql` :

```sql
CREATE DATABASE IF NOT EXISTS perf;
USE perf;
SHOW TABLES;

SELECT * FROM sales_parquet LIMIT 10;

SELECT product, COUNT(*) AS n, SUM(amount) AS total_amount
FROM sales_parquet
GROUP BY product;

EXIT;
```

### 5.4 Lire CSV et sérialiser en Parquet (manuel, PySpark)

```powershell
docker exec -it spark-master-new /spark/bin/pyspark --master spark://spark-master-new:7077
```

Dans le shell PySpark :

```python
df = spark.read.csv("hdfs://namenode:8020/user/data/large_sales.csv",
                    header=True, inferSchema=True)
df.show(5)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/user/output/sales_parquet")
spark.stop()
```

---

## 6. Benchmark Parquet vs HBase

### 6.1 Préparer HBase (créer la table) – version PowerShell

#### Option A – Mode interactif

```powershell
docker exec -it hbase-master hbase shell
```

Dans le shell HBase :

```text
disable 'sales'   # si la table existe
drop 'sales'      # si la table existe

create 'sales', 'cf'
list
exit
```

#### Option B – One-liner (non interactif, facultatif)

```powershell
docker exec hbase-master bash -lc "echo "disable 'sales'" | hbase shell"
docker exec hbase-master bash -lc "echo "drop 'sales'"    | hbase shell"
docker exec hbase-master bash -lc "echo "create 'sales', 'cf'" | hbase shell"
docker exec hbase-master /bin/sh -lc "echo 'list' | /hbase/bin/hbase shell"
```

### 6.2 Copier les scripts de benchmark

```powershell
docker cp .\benchmarks\benchmark_hbase.py             spark-master-new:/benchmark_hbase.py
docker cp .\benchmarks\benchmark_parquet_complete.py  spark-master-new:/benchmark_parquet_complete.py
docker cp .\benchmarks\benchmark_config.py            spark-master-new:/benchmark_config.py
docker cp .\benchmarks\generate_comparison.py         spark-master-new:/generate_comparison.py

```

### 6.3 Exécuter le benchmark HBase

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  --files /spark/conf/hbase-site.xml `
  --packages org.apache.hbase:hbase-client:2.1.3,org.apache.hbase:hbase-common:2.1.3 `
  /benchmark_hbase.py
```

### 6.4 Exécuter le benchmark Parquet

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  /benchmark_parquet_complete.py
```

### 6.5 Générer le tableau comparatif

```powershell
docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  /generate_comparison.py
```

### 6.6 Voir les métriques HBase et Parquet

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e "SELECT * FROM perf.hbase_metrics;"
docker exec spark-master-new /spark/bin/spark-sql -e "SELECT * FROM perf.parquet_metrics;"
docker exec spark-master-new /spark/bin/spark-sql -e "SELECT * FROM perf.comparison_results;"
```

### 6.7 Script automatisé complet (Parquet vs HBase)

```powershell
bash ./scripts/run_full_benchmark.sh
```

---

## 7. Spark ML - Benchmark des Modèles   " consulter ./spark_ml/Readme.md si il ya des problemes "

### 7.1 Vérifier que les données sont en Parquet dans Hive

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e "SHOW TABLES IN perf;"
docker exec spark-master-new /spark/bin/spark-sql -e "SELECT COUNT(*) FROM perf.sales_parquet;"
```

### 7.2 Vérifier la présence du script ML

```powershell
docker exec spark-master-new ls -la /spark_ml/
```

### 7.3 Exécuter le benchmark Spark ML

```powershell
docker exec -it spark-master-new bash
apk update
apk add --no-cache py3-numpy 

apk add --no-cache py3-pip
pip3 install ydata-profiling
exit ;

docker exec spark-master-new /spark/bin/spark-submit `
  --master spark://spark-master-new:7077 `
  /spark_ml/ml_benchmark.py `
  --table perf.sales_parquet `
  --dataset sales `
  --source-format parquet `
  --split-strategy random `
  --label-quantile 0.75
```

### 7.4 Voir les résultats ML

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e `
"SELECT 
    model, 
    ROUND(auc_roc, 4) AS auc_roc,
    ROUND(accuracy, 4) AS accuracy,
    ROUND(precision, 4) AS precision,
    ROUND(recall, 4) AS recall,
    ROUND(f1, 4) AS f1,
    ROUND(train_time_s, 2) AS train_time_s
  FROM perf.ml_benchmark_results
  ORDER BY auc_roc DESC;"
`
```

### 7.5 Exporter les résultats ML en CSV

```powershell
# Export via script Python (déjà monté dans le conteneur)
docker exec spark-master-new /spark/bin/spark-submit /tmp/export_ml_results.py

# OU, avec le script d'automatisation :
bash ./scripts/run_spark_ml.sh
```

### 7.6 Récupérer les résultats CSV localement

```powershell
# Copier les CSV générés depuis le conteneur vers la machine hôte
docker cp spark-master-new:/spark_ml/output_ml_results/. .\ml_results_csv
# Voir rapidement le contenu
Get-ChildItem .\ml_results_csvGet-Content .\ml_results_csv\part-00000*.csv | Select-Object -First 30
```

---

## 8. Consultation des Résultats

### 8.1 Voir toutes les bases et tables Hive

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e "SHOW DATABASES;"
docker exec spark-master-new /spark/bin/spark-sql -e "USE perf; SHOW TABLES;"
```

### 8.2 Résultats du benchmark des formats

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e `
"SELECT 
    dataset,
    format,
    rows,
    ROUND(write_time_s, 2) AS write_s,
    ROUND(read_time_s, 2) AS read_s,
    ROUND(size_mb, 2) AS size_mb
  FROM perf.format_benchmark_results
  ORDER BY dataset, write_time_s;"
`
```

### 8.3 Résultats comparatifs Parquet vs HBase

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e "SELECT * FROM perf.comparison_results;"
```

### 8.4 Résultats du benchmark ML

```powershell
docker exec spark-master-new /spark/bin/spark-sql -e `
"SELECT 
    model,
    source_format,
    ROUND(auc_roc, 4) AS auc_roc,
    ROUND(auc_pr, 4) AS auc_pr,
    ROUND(accuracy, 4) AS accuracy,
    ROUND(f1, 4) AS f1,
    ROUND(train_time_s, 2) AS train_s,
    n_train,
    n_test
  FROM perf.ml_benchmark_results
  ORDER BY auc_roc DESC;"
`
```

### 8.5 Vérifier les fichiers sur HDFS

```powershell
docker exec namenode hdfs dfs -ls -R /user/output/
docker exec namenode hdfs dfs -du -h /user/output/
```

---

## 9. Commandes Utiles

### 9.1 Accéder aux interfaces Web

| Service         | URL                         |
|----------------|-----------------------------|
| HDFS NameNode  | http://localhost:9870      |
| Spark Master   | http://localhost:8080      |
| Spark Worker   | http://localhost:8081      |
| HBase Master   | http://localhost:16010     |

### 9.2 Shells interactifs

```powershell
# Shell PySpark
docker exec -it spark-master-new /spark/bin/pyspark --master spark://spark-master-new:7077

# Shell Spark SQL
docker exec -it spark-master-new /spark/bin/spark-sql

# Shell HBase
docker exec -it hbase-master hbase shell

# Shell NameNode (bash + HDFS)
docker exec -it namenode bash
```

### 9.3 Voir les logs

```powershell
docker logs spark-master-new
docker logs hbase-master
docker logs hive-metastore
```

### 9.4 Copier des fichiers

```powershell
# Local -> conteneur
docker cp .\mon_fichier.py spark-master-new:/mon_fichier.py

# Conteneur -> local
docker cp spark-master-new:/chemin/dans/conteneur/fichier.csv .\fichier.csv

# HDFS -> local (via NameNode)
docker exec namenode hdfs dfs -get /user/data/fichier.csv /tmp/fichier.csv
docker cp namenode:/tmp/fichier.csv .\

---

## 10. Arrêt et Nettoyage

### 10.1 Arrêter les services (sans supprimer les données)

```powershell
docker-compose stop
```

### 10.2 Redémarrer les services

```powershell
docker-compose start
```

### 10.3 Arrêter et supprimer les conteneurs

```powershell
docker-compose down
```

### 10.4 Supprimer tout (conteneurs + volumes) ⚠️

```powershell
docker-compose down -v
```

### 10.5 Nettoyer les données HDFS / Hive

```powershell
# Supprimer les fichiers de sortie HDFS
docker exec namenode hdfs dfs -rm -r /user/output/*

# Supprimer la base perf dans Hive
docker exec spark-master-new /spark/bin/spark-sql -e "DROP DATABASE IF EXISTS perf CASCADE;"
```

---

## 📊 Résumé des Tables Hive Créées

| Table                         | Description                                          |
|------------------------------|------------------------------------------------------|
| `perf.format_benchmark_results` | Benchmark CSV / Avro / ORC / Parquet              |
| `perf.sales_parquet`         | Données de ventes en Parquet                        |
| `perf.hbase_metrics`         | Métriques benchmark HBase                           |
| `perf.parquet_metrics`       | Métriques benchmark Parquet                         |
| `perf.comparison_results`    | Comparaison Parquet vs HBase                        |
| `perf.ml_benchmark_results`  | Résultats Spark ML                                  |
| `perf.ml_input_summary`      | Stats des données d'entrée pour Spark ML            |

---

## 🎯 Ordre d'Exécution Recommandé (PowerShell)

1. `docker-compose up -d`  
2. (optionnel) `bash .\scripts\verify_services.sh`  
3. `python .\scripts\generate_data.py .\data\large_sales.csv 1000000`  
4. Upload HDFS (section 3)  
5. `bash .\scripts
un_format_benchmark.sh`  
6. `bash .\scripts
un_full_benchmark.sh`  
7. `bash .\scripts
un_spark_ml.sh`  
8. Consulter les résultats (section 8)

---

**Auteur** : Projet Big Data - ENSA El Jadida  
**Date** : 2024-2025
