# Tutoriel : Comparaison des Formats Big Data et Benchmark

Ce document détaille les étapes réalisées pour comparer trois formats de stockage (CSV, Parquet, HBase) dans un environnement Big Data (Hadoop, Hive, Spark, HBase) et explique comment utiliser Hive avec Spark.

## Partie 1 : Comparaison des Formats dans Hive et Utilisation avec Spark

### 1. Architecture
Nous utilisons une architecture conteneurisée avec Docker Compose comprenant :
- **Hadoop (HDFS)** : Stockage distribué.
- **Hive** : Entrepôt de données pour structurer les fichiers HDFS via SQL.
- **HBase** : Base de données NoSQL orientée colonnes pour les accès aléatoires rapides.
- **Spark** : Moteur de traitement unifié pour les calculs et le benchmark.

### 2. Les Trois Formats
1.  **CSV (Comma Separated Values)** :
    -   *Avantages* : Simple, lisible par l'humain, universel.
    -   *Inconvénients* : Pas de compression, pas de schéma strict, lent à lire (parsing texte), pas de pushdown predicate.
    -   *Usage* : Échange de données brutes.

2.  **Parquet** :
    -   *Avantages* : Format colonnaire, forte compression (Snappy/Gzip), schéma intégré, très rapide pour les requêtes analytiques (OLAP) grâce au "projection pushdown" (lire seulement les colonnes nécessaires).
    -   *Inconvénients* : Écriture plus coûteuse que le CSV, binaire (non lisible directement).
    -   *Usage* : Data Lakes, Analytics, Business Intelligence.

3.  **HBase** :
    -   *Avantages* : Accès aléatoire temps réel (clé/valeur), mises à jour possibles, scalabilité massive.
    -   *Inconvénients* : Complexe à gérer, moins performant pour les scans complets (OLAP) que Parquet.
    -   *Usage* : Applications temps réel, Serving Layer.

### 3. Utilisation de Hive dans Spark
Pour que Spark puisse interagir avec Hive (lire/écrire des tables), nous avons configuré `SparkSession` avec `.enableHiveSupport()`.

**Configuration Clé :**
-   `hive-site.xml` doit être présent dans le dossier `conf` de Spark.
-   Propriété `hive.metastore.uris` : pointe vers le service `hive-metastore` (ex: `thrift://hive-metastore:9083`).
-   Jars : Spark doit avoir les drivers Hive (inclus par défaut dans les images Spark compatibles Hadoop).

**Exemple de Code (Python/PySpark) :**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Créer une base de données Hive
spark.sql("CREATE DATABASE IF NOT EXISTS ma_base")

# Lire une table Hive
df = spark.sql("SELECT * FROM ma_base.ma_table")

# Écrire un DataFrame dans une table Hive
df.write.mode("overwrite").saveAsTable("ma_base.nouvelle_table")
```

---

## Partie 2 : Benchmark Parquet vs HBase

Nous avons développé une suite de scripts pour mesurer les performances de lecture, écriture et requête sur ces formats.

### 1. Méthodologie
Le benchmark exécute les opérations suivantes pour chaque format :
1.  **Ingestion** : Lecture d'un fichier CSV source.
2.  **Écriture** : Conversion et écriture dans le format cible (Parquet sur HDFS ou Table HBase).
3.  **Lecture** : Relecture complète des données.
4.  **Requêtes SQL** :
    -   `SELECT *` (Scan complet)
    -   `FILTER` (Filtrage sur une colonne)
    -   `GROUP BY` (Agrégation)
    -   `COUNT` (Comptage)
5.  **Stockage des Métriques** : Les temps d'exécution sont enregistrés dans des tables Hive (`perf.hbase_metrics`, `perf.parquet_metrics`).

### 2. Scripts du Benchmark
Les scripts ont été organisés dans le dossier `benchmarks/` :

-   `benchmark_hbase.py` :
    -   Utilise le connecteur `HBase-Spark` (via `shc-core`).
    -   Définit un catalogue JSON pour mapper les colonnes HBase aux colonnes Spark DataFrame.
    -   Mesure les temps d'insertion (`put`) et de scan.

-   `benchmark_parquet_complete.py` :
    -   Lit le CSV et écrit en Parquet (`df.write.parquet`).
    -   Crée une table Hive externe pointant vers les fichiers Parquet.
    -   Exécute des requêtes SQL sur cette table.

-   `generate_comparison.py` :
    -   Lit les deux tables de métriques dans Hive.
    -   Joint les résultats sur le type d'opération.
    -   Calcule le "Speedup" (Ratio de performance).
    -   Génère un rapport Markdown et un CSV.

-   `read_csv_hive.py` : Script utilitaire pour vérifier la lecture CSV via Hive.

### 3. Résultats et Interprétation (Petit Jeu de Données)
Les résultats ci-dessous ont été obtenus sur un petit jeu de données (`sample_sales.csv`, 5 lignes).

| Opération | HBase (ms) | Parquet (ms) | Différence (%) |
| :--- | :--- | :--- | :--- |
| **Écriture** | 4559 | 1973 | +131% (Parquet plus rapide) |
| **Lecture (Scan)** | 217 | - | - |
| **SQL Count** | 308 | 516 | -40% (HBase plus rapide) |
| **SQL Filter** | 702 | 971 | -27% (HBase plus rapide) |
| **SQL GroupBy** | 7099 | 9430 | -24% (HBase plus rapide) |
| **SQL Select All** | 662 | 792 | -16% (HBase plus rapide) |

**Interprétation Préliminaire :**
Sur ce très petit volume de données, les résultats sont contre-intuitifs par rapport à la théorie Big Data :
1.  **Écriture** : Parquet est nettement plus rapide. L'écriture dans HBase implique des coûts fixes (connexion Zookeeper, gestion des WAL) qui sont lourds pour seulement 5 lignes.
2.  **Lecture SQL** : HBase semble plus performant ici. Cela s'explique probablement par le fait que Spark doit initialiser le lecteur Parquet et inferer le schéma, ce qui prend un temps constant incompressible. Pour HBase, une fois la connexion établie, récupérer 5 clés est instantané.
3.  **Conclusion** : Ces résultats mesurent surtout les "coûts d'initialisation" (overhead) des deux systèmes. Pour observer les vrais gains de performance de Parquet (compression, scan colonnaire) et de HBase (accès aléatoire), il est nécessaire de passer à un jeu de données plus volumineux (plusieurs millions de lignes).

### 4. Comment Exécuter le Benchmark
Un script maître `scripts/run_full_benchmark.sh` orchestre tout le processus :

```bash
./scripts/run_full_benchmark.sh
```

Ce script :
1.  Vérifie l'état des services Docker.
2.  Prépare HBase (création de table).
3.  Upload les données de test (`data/sample_sales.csv`) sur HDFS.
4.  Lance `benchmark_hbase.py` via `spark-submit`.
5.  Lance `benchmark_parquet_complete.py` via `spark-submit`.
6.  Génère le rapport comparatif.

---

## Partie 3 : Comparaison Avro, ORC, Parquet

Bien que le benchmark principal se concentre sur Parquet vs HBase, il est essentiel de comprendre les différences avec les autres formats majeurs de l'écosystème Hadoop : Avro et ORC.

### 1. Caractéristiques des Formats

| Format | Type | Schéma | Compression | Cas d'usage idéal |
|--------|------|--------|-------------|-------------------|
| **CSV** | Texte | Non (Inferred) | Faible (Gzip non splitable) | Échange de données, compatibilité universelle |
| **Parquet** | Colonnaire | Oui (Intégré) | Excellente (Snappy, Gzip) | **Analytique (OLAP)** : Lecture de quelques colonnes sur beaucoup de lignes |
| **ORC** | Colonnaire | Oui (Intégré) | Excellente (Zlib, Snappy) | **Analytique (Hive)** : Très optimisé pour Hive, supporte les transactions ACID |
| **Avro** | Ligne | Oui (JSON) | Bonne | **Écriture (OLTP)** : Ingestion rapide, évolution de schéma, Kafka |

### 2. Implémentation dans Spark

Pour comparer ces formats, voici la logique d'implémentation standard (basée sur notre plan initial) :

#### Prérequis
Pour utiliser Avro avec Spark < 2.4 (ou Spark 3+ externe), il faut inclure le package `spark-avro` :
```bash
--packages org.apache.spark:spark-avro_2.12:3.1.1
```

#### Code de Benchmark (Python)

```python
# 1. Lecture du CSV source
df = spark.read.csv("/user/data/sample_sales.csv", header=True, inferSchema=True)

# 2. Écriture dans les différents formats
# Parquet
start = time.time()
df.write.mode("overwrite").parquet("/user/data/sales_parquet")
print(f"Écriture Parquet: {time.time() - start}s")

# ORC
start = time.time()
df.write.mode("overwrite").orc("/user/data/sales_orc")
print(f"Écriture ORC: {time.time() - start}s")

# Avro
start = time.time()
df.write.format("avro").mode("overwrite").save("/user/data/sales_avro")
print(f"Écriture Avro: {time.time() - start}s")

# 3. Lecture et Comparaison
# Lecture Parquet
start = time.time()
spark.read.parquet("/user/data/sales_parquet").count()
print(f"Lecture Parquet (Count): {time.time() - start}s")

# Lecture ORC
start = time.time()
spark.read.orc("/user/data/sales_orc").count()
print(f"Lecture ORC (Count): {time.time() - start}s")

# Lecture Avro
start = time.time()
spark.read.format("avro").load("/user/data/sales_avro").count()
print(f"Lecture Avro (Count): {time.time() - start}s")
```

### 3. Résultats du Benchmark (Dataset 1M lignes)

Voici les résultats obtenus sur un dataset de 1 million de lignes (généré aléatoirement) :

| Format | Taille (MB) | Temps Écriture (s) | Temps Lecture (Count) (s) |
|--------|-------------|--------------------|---------------------------|
| **CSV** | 31.13 | 9.37 | ~1 |
| **Avro** | 14.70 | 9.80 | ~4 |
| **Parquet** | 10.19 | 14.74 | ~2 |
| **ORC** | 7.03 | 6.90 | ~9 |

*(Note : Les temps de lecture sont arrondis)*

### 4. Interprétation et Choix du Format

#### 📊 Analyse des Résultats
1.  **Stockage (Compression)** :
    -   **ORC** et **Parquet** sont les grands gagnants, réduisant la taille de **~65-75%** par rapport au CSV.
    -   **Avro** offre une compression intermédiaire (~50%).
    -   **CSV** est le plus volumineux.

2.  **Performance d'Écriture** :
    -   **ORC** a été le plus rapide dans ce test, suivi de près par le **CSV** (qui n'a aucun surcoût d'encodage).
    -   **Parquet** est le plus lent à écrire (+50% de temps vs CSV), ce qui est normal car il effectue un encodage complexe (Dremel) et une compression lourde pour optimiser les lectures futures.

3.  **Performance de Lecture** :
    -   **Parquet** est très performant pour la lecture.
    -   **CSV** est rapide pour un simple comptage séquentiel, mais deviendrait très lent pour des requêtes complexes (filtrage, agrégation) car il faut parser chaque ligne textuelle.

#### 💡 Quand utiliser quoi ?

-   **Utilisez Parquet** pour les **Data Lakes** et l'analyse (BI, Data Science). C'est le standard pour la lecture rapide de gros volumes (OLAP). Le surcoût à l'écriture est largement rentabilisé par les gains en lecture et stockage.

-   **Utilisez Avro** pour l'**ingestion de données** (Streaming, Kafka) et les pipelines d'écriture intensive (OLTP). Il gère très bien l'évolution des schémas (ajout de colonnes).

-   **Utilisez ORC** si vous travaillez exclusivement dans l'écosystème **Hive**. Il est ultra-optimisé pour Hive et supporte les transactions ACID.

-   **Utilisez CSV** uniquement pour l'**échange de données** avec des systèmes externes ou pour le débogage humain. À bannir pour le stockage long terme ou le traitement Big Data.
