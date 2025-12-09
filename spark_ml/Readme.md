# Projet Big Data – Comparaison Avro / ORC / Parquet + Spark ML

Ce projet met en place une infrastructure Big Data complète (Hadoop, Hive, Spark, HBase) dans Docker pour :

- comparer les formats **Avro**, **ORC** et **Parquet** (temps d’écriture / lecture),
- comparer **Parquet vs HBase** via Spark SQL,
- lire un fichier **CSV** et le sérialiser en Parquet avec Hive/Spark,
- utiliser **Spark ML** pour entraîner plusieurs modèles et produire un **tableau comparatif de performances**,
- enregistrer ces performances dans une **table Hive**.

La partie **Spark ML** est implémentée dans le script :

> `spark-master-new:/spark_ml/ml_benchmark.py`

---

## 1. Prérequis

- Docker installé
- (facultatif) docker-compose
- Machine avec au moins **8 Go de RAM**

---

## 2. Démarrer l’infrastructure Big Data

Depuis la machine hôte (Windows / Linux / macOS) :

```bash
cd bigdata_project


docker-compose up -d
```


### Vérifier que les conteneurs sont bien lancés :
```bash
docker ps
```



### Vous devez voir au moins les conteneurs suivants (noms approximatifs) :

namenode

datanode

spark-master-new

spark-worker-*

hive-metastore, hive-server, etc.

hbase (si activé dans le projet)

## 3. Préparation des données (CSV → Parquet / Avro / ORC / HBase)

⚠️ Les scripts de génération de données et de comparaison de formats
sont déjà fournis dans l’archive .tar du projet (scripts HDFS, Hive,
Avro/ORC/Parquet, HBase, etc.).

En général, le scénario est le suivant :

Un fichier CSV est copié dans HDFS, par exemple :

/user/data/sample_sales.csv

/user/data/large_sales.csv

Des scripts (fournis dans le projet) comparent les formats :

CSV → Parquet

CSV → ORC

CSV → Avro

et mesurent les temps d’écriture / lecture via Spark SQL + Hive.

Un autre script compare Parquet vs HBase :

écriture / lecture,

temps d’exécution des requêtes.

Pour ces parties, se référer aux scripts d’origine livrés avec le sujet
(ils sont indépendants de ml_benchmark.py).

## 4. Installation des librairies Python dans le conteneur Spark

On se connecte au conteneur spark-master-new :
```bash



MSYS_NO_PATHCONV=1 docker exec -it spark-master-new bash

```
### 4.1. Installer NumPy (déjà utilisé)

L’image Spark est basée sur Alpine, on utilise apk :

apk update
apk add --no-cache py3-numpy

4.2. (Optionnel) Installer ydata-profiling pour le profiling avancé

Si vous souhaitez générer un rapport HTML de profiling :

apk add --no-cache py3-pip
pip3 install ydata-profiling


Si ydata_profiling n’est pas installé, ml_benchmark.py affiche simplement un warning et continue sans générer de rapport.

## 5. Table Hive source pour Spark ML

Le script ml_benchmark.py suppose l’existence d’une table Hive de type Parquet, par exemple :

HDFS : /user/output/huge_parquet/

Table Hive externe : perf.sales_parquet

Création de la table depuis spark-master-new :

```bash 
MSYS_NO_PATHCONV=1 docker exec -it spark-master-new bash

/spark/bin/spark-sql


```
Dans la console spark-sql :

-- 1) Créer la base si elle n'existe pas
CREATE DATABASE IF NOT EXISTS perf;

-- 2) Créer une table PARQUET pointant sur le dossier HDFS déjà existant
DROP TABLE IF EXISTS perf.sales_parquet;

CREATE TABLE perf.sales_parquet
USING PARQUET
LOCATION 'hdfs://namenode:8020/user/output/huge_parquet';

-- 3) Vérifier
SHOW TABLES IN perf;
SELECT COUNT(*) FROM perf.sales_parquet;


Sortir de spark-sql :

exit;

## 6. Script Spark ML : ml_benchmark.py

Le script ml_benchmark.py :

lit la table perf.sales_parquet,

prépare les features :

conversion de la date en timestamp

extraction de month, day_of_week

construit un label binaire à partir de la colonne amount

label = 1 si amount ≥ quantile --label-quantile (par défaut 0.75)

gère le déséquilibre de classes via des poids de classe

entraîne et compare trois modèles :

logistic_regression

random_forest

gbt_classifier

calcule les métriques :

auc_roc, auc_pr

accuracy, precision, recall, f1

temps d’entraînement et de test

enregistre les résultats dans une table Hive :

perf.ml_benchmark_results (format Parquet)

### 6.1. Lancer le benchmark ML

Toujours dans le conteneur spark-master-new :

```bash 
MSYS_NO_PATHCONV=1 docker exec -it spark-master-new bash

cd /spark_ml

PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
  /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  ml_benchmark.py \
  --table perf.sales_parquet

```

⚠️ Remarque :
La commande ci-dessus lance seulement le script ML.
Pour interroger la table de résultats, il faut exécuter une commande spark-sql séparée (voir ci-dessous).

### 6.2. Options utiles

--sample-frac : fraction des données à utiliser (par ex. 0.3 pour un échantillon de 30 %)

--split-strategy : random (par défaut) ou temporal

--label-quantile : quantile pour définir le label (par défaut 0.75)

--profile-html : chemin vers un rapport HTML YData Profiling (optionnel)

Exemples :

```bash 
# Échantillon 30 %, split temporel
PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
  /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  ml_benchmark.py \
  --table perf.sales_parquet \
  --sample-frac 0.3 \
  --split-strategy temporal

```

```bash
# Avec rapport YData Profiling
PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
  /spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  ml_benchmark.py \
  --table perf.sales_parquet \
  --profile-html /spark/reports/sales_profile.html

```
## 7. Consulter les résultats dans Hive

Après exécution de ml_benchmark.py, les résultats sont dans la table :

perf.ml_benchmark_results

Pour les visualiser :

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master-new bash

/spark/bin/spark-sql -e "SELECT * FROM perf.ml_benchmark_results LIMIT 50;"


```
Vous pouvez également inspecter le schéma :

```bash
/spark/bin/spark-sql -e "DESCRIBE FORMATTED perf.ml_benchmark_results;"


```
Les colonnes incluent notamment :

dataset, source_format

model

train_time_s, test_time_s

auc_roc, auc_pr

accuracy, precision, recall, f1

n_train, n_test

pos_ratio_train, pos_ratio_test

split_strategy, label_quantile, label_threshold_amount

params_json (hyperparamètres du modèle)

