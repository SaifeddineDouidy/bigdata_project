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

### 3. Résultats et Interprétation
Les résultats sont stockés dans `benchmark_results/`.

**Observations Générales :**
-   **Écriture** : Parquet est généralement plus rapide car il s'agit d'une écriture séquentielle de fichiers, tandis que HBase doit gérer les WAL et la distribution des régions.
-   **Lecture Analytique (Group By, Count)** : Parquet surpasse largement HBase sur les gros volumes grâce au format colonnaire.
-   **Accès Unitaire** : HBase est imbattable pour récupérer une ligne spécifique par sa clé (RowKey), ce que Parquet ne peut faire qu'en scannant le fichier (ou via des index partitionnés).

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

### 3. Résultats Attendus

-   **Vitesse d'écriture** : Avro > Parquet ≈ ORC (Avro est optimisé pour l'écriture ligne par ligne).
-   **Vitesse de lecture (Scan complet)** : Parquet ≈ ORC > Avro.
-   **Vitesse de lecture (Quelques colonnes)** : Parquet/ORC >>> Avro (grâce au format colonnaire).
-   **Taille de stockage** : ORC ≈ Parquet < Avro < CSV.
