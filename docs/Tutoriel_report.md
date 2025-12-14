# Rapport - Tutoriel : Benchmarks Parquet vs HBase et Spark ML

Ce document est un tutoriel pas-à-pas décrivant les étapes suivies pour :
- ingérer des CSV dans HDFS et les exposer via Hive,
- mesurer et comparer les performances des formats (Parquet, ORC, Avro, CSV),
- mesurer et comparer Parquet vs HBase (écriture/lecture et requêtes Spark SQL),
- exécuter un pipeline Spark ML pour comparer plusieurs modèles et sauver les métriques.

**But du tutoriel :** permettre à un lecteur de reproduire l'expérience complète et de comprendre où chercher les résultats.

**Pré-requis**
- Docker & Docker Compose configurés pour lancer un cluster local avec : Namenode (HDFS), Hive metastore, Spark master, HBase, Zookeeper.
- Les scripts attendent des conteneurs nommés (par défaut) : `spark-master-new`, `namenode`, `hbase-master`, `zookeeper`. Adaptez si nécessaire.
- Spark avec support Hive, et les packages Avro / HBase client disponibles ou passés via `--packages`.

**Structure du dépôt (rapide rappel)**
- `benchmarks/` — scripts de benchmark
- `spark_ml/` — pipeline ML
- `scripts/` — orchestrateurs Bash
- `data/` — datasets CSV
- `docs/` — documentation (ce fichier)

---

**Étape 0 — Préparer le dataset sur HDFS**

1. Copier `data/sample_sales.csv` (ou `data/large_sales.csv`) sur HDFS (ex : `/user/data/`).
   Exemple (depuis host, en présumant conteneur `namenode`):

```bash
# copier un fichier sur le conteneur namenode, puis mettre sur HDFS
docker cp data/sample_sales.csv namenode:/tmp/sample_sales.csv
docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -put -f /tmp/sample_sales.csv /user/data/sample_sales.csv
```

2. Vérifier la présence sur HDFS :

```bash
docker exec namenode hdfs dfs -ls /user/data
```

---

**Étape 1 — Benchmark multi-format (CSV/Parquet/ORC/Avro)**

- Script : `benchmarks/benchmark_formats.py`
- Objectif : lire le CSV source et mesurer les temps d'écriture et de lecture pour chaque format, calculer la taille sur HDFS et sauvegarder les résultats dans la table Hive `perf.format_benchmark_results`.

Exécution (exemple via le conteneur Spark) :

```bash
# copier le script dans le conteneur spark, puis exécuter
docker cp benchmarks/benchmark_formats.py spark-master-new:/benchmark_formats.py
docker exec spark-master-new /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  --packages org.apache.spark:spark-avro_2.12:3.1.1 \
  /benchmark_formats.py \
  --input-file "hdfs://namenode:8020/user/data/sample_sales.csv" \
  --dataset-name "small"
```

Résultat : table Hive `perf.format_benchmark_results` contenant pour chaque dataset/format : rows, write_time_s, read_time_s, size_mb.

---

**Étape 2 — Benchmark Parquet complet**

- Script : `benchmarks/benchmark_parquet_complete.py`
- Objectif : écrire/relire en Parquet, créer table Hive externe pointant vers le dossier Parquet, exécuter plusieurs requêtes Spark SQL (SELECT, FILTER, GROUP BY, COUNT, complex) et sauvegarder les métriques dans `perf.parquet_metrics`.

Exécution :

```bash
docker cp benchmarks/benchmark_parquet_complete.py spark-master-new:/benchmark_parquet_complete.py
docker exec spark-master-new /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  /benchmark_parquet_complete.py
```

Sorties : Hive table `perf.parquet_metrics`, logs affichant tailles/temps et exemples de résultats.

---

**Étape 3 — Benchmark HBase complet**

- Script : `benchmarks/benchmark_hbase.py`
- Objectif : insérer les données dans HBase (via API Java depuis PySpark), mesurer écritures sur plusieurs itérations, scanner/compter, convertir en DataFrame Spark, exécuter requêtes Spark SQL (après conversion), puis sauvegarder les métriques dans `perf.hbase_metrics`.

Pré-requis : HBase doit être accessible par le job Spark (vérifier `hbase-site.xml` et jars). Le script utilise `benchmark_config.py` pour les paramètres.

Exécution :

```bash
docker cp benchmarks/benchmark_hbase.py spark-master-new:/benchmark_hbase.py
# on passe hbase-site.xml si nécessaire via --files
docker exec spark-master-new /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  --files /spark/conf/hbase-site.xml \
  --packages org.apache.hbase:hbase-client:2.1.3,org.apache.hbase:hbase-common:2.1.3 \
  /benchmark_hbase.py
```

Sorties : Hive table `perf.hbase_metrics`.

---

**Étape 4 — Génération du tableau comparatif Parquet vs HBase**

- Script : `benchmarks/generate_comparison.py`
- Objectif : lire `perf.hbase_metrics` et `perf.parquet_metrics`, joindre sur l'opération, calculer `speedup`, `time_diff_ms`, `time_diff_percent`, sauvegarder la table Hive `perf.comparison_results` et exporter un CSV et un rapport markdown.

Exécution :

```bash
docker cp benchmarks/generate_comparison.py spark-master-new:/generate_comparison.py
docker exec spark-master-new /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  /generate_comparison.py
```

Sorties :
- Hive table `perf.comparison_results`.
- Local CSV `benchmark_results/comparison_results.csv` (si container copie disponible).
- Markdown report `benchmark_results/benchmark_report.md`.

---

**Étape 5 — Pipeline Spark ML (comparatif de modèles)**

- Script : `spark_ml/ml_benchmark.py`
- Objectif : lire une table Hive source (par défaut `perf.sales_parquet`), faire feature engineering, définir label binaire (quantile), entraîner plusieurs modèles (Logistic Regression, Random Forest, GBT) avec cross-validation, calculer métriques (AUC ROC, AUC PR, accuracy, precision, recall, f1), et sauvegarder les résultats dans `perf.ml_benchmark_results`.

Exécution :

```bash
# Exemple : lancer sur la table Parquet créée précédemment
/spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  spark_ml/ml_benchmark.py \
  --table perf.sales_parquet \
  --dataset sales \
  --sample-frac 1.0
```

Sorties : Hive tables `perf.ml_benchmark_results` et `perf.ml_input_summary`.

---

**Requêtes utiles pour vérifier les résultats**

Se connecter au shell Spark SQL (dans le conteneur Spark) :

```bash
docker exec -it spark-master-new /spark/bin/spark-sql
# puis dans le shell spark-sql:
USE perf;
SELECT * FROM format_benchmark_results LIMIT 10;
SELECT * FROM parquet_metrics LIMIT 10;
SELECT * FROM hbase_metrics LIMIT 10;
SELECT * FROM comparison_results LIMIT 10;
SELECT * FROM ml_benchmark_results LIMIT 10;
```

---

**Conseils & pièges courants**
- Versions des packages : Assurez-vous que les versions des packages passés à `--packages` correspondent à la version de Spark utilisée (scala version compatibilité).
- Jars HBase : pour interagir avec HBase depuis Spark (benchmark_hbase.py), placez correctement les jars HBase sur le classpath ou passez via `--packages`.
- Ressources Spark : les benchmarks peuvent nécessiter de la mémoire CPU; ajustez `--conf spark.driver.memory` et `--conf spark.executor.memory` selon besoin.
- Itérations : augmenter `BENCHMARK_ITERATIONS` dans `benchmarks/benchmark_config.py` pour mesurer min/max/moyen de façon significative.

---

**Emplacement des fichiers générés**
- Hive tables: `perf.*` (voir liste dans README).
- Locally: `benchmark_results/comparison_results.csv`, `benchmark_results/benchmark_report.md` (si `generate_comparison.py` crée et copie localement).

---

**Conclusion**
Ce tutoriel couvre l'ensemble du flux expérimental présent dans le dépôt : ingestion, conversion, benchmarks multi-format, comparaison Parquet vs HBase, et benchmarking ML. Les scripts sont prêts à l'exécution dans un environnement Docker Compose correctement configuré ; mettez à jour `benchmark_config.py` et `scripts/*` pour adapter les noms d'hôtes/containers et l'itération souhaitée.

Si vous voulez, je peux maintenant:
- exécuter un test de cohérence des CREATE TABLE SQL (lister les requêtes créées par les scripts),
- ajouter un diagramme SVG simple sous `docs/architecture.svg`, ou
- ajuster `BENCHMARK_ITERATIONS` et commiter la modification. Lequel préférez-vous ?