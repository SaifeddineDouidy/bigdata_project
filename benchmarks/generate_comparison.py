"""
Génère le tableau comparatif final entre Parquet et HBase
- Joint les deux tables de métriques
- Génère DataFrame comparatif
- Exporte CSV
- Crée table Hive perf.comparison_results
- Génère rapport markdown
"""
import time
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round

# Import de la configuration
sys.path.append('.')
try:
    from benchmark_config import *
except ImportError:
    HIVE_DATABASE = "perf"
    HIVE_TABLE_HBASE_METRICS = "hbase_metrics"
    HIVE_TABLE_PARQUET_METRICS = "parquet_metrics"
    HIVE_TABLE_COMPARISON = "comparison_results"
    OUTPUT_DIR = "./benchmark_results"
    COMPARISON_CSV = "./benchmark_results/comparison_results.csv"
    COMPARISON_REPORT = "./benchmark_results/benchmark_report.md"

print("=" * 60)
print("GÉNÉRATION DU TABLEAU COMPARATIF")
print("=" * 60)

# Initialisation Spark avec support Hive
spark = SparkSession.builder \
    .appName("benchmark_comparison") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# Créer le répertoire de sortie si nécessaire
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# 1. LECTURE DES MÉTRIQUES
# ============================================================
print("\n1. Lecture des métriques HBase et Parquet...")

try:
    df_hbase = spark.sql(f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE_HBASE_METRICS}")
    print(f"✓ Métriques HBase: {df_hbase.count()} lignes")
    df_hbase.show(truncate=False)
except Exception as e:
    print(f"✗ Erreur lors de la lecture des métriques HBase: {e}")
    df_hbase = None

try:
    df_parquet = spark.sql(f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE_PARQUET_METRICS}")
    print(f"✓ Métriques Parquet: {df_parquet.count()} lignes")
    df_parquet.show(truncate=False)
except Exception as e:
    print(f"✗ Erreur lors de la lecture des métriques Parquet: {e}")
    df_parquet = None

if df_hbase is None or df_parquet is None:
    print("ERREUR: Impossible de lire les métriques. Vérifiez que les benchmarks ont été exécutés.")
    spark.stop()
    exit(1)

# ============================================================
# 2. PRÉPARATION DES DONNÉES POUR COMPARAISON
# ============================================================
print("\n2. Préparation des données pour comparaison...")

# Sélectionner les colonnes pertinentes et ajouter un préfixe
df_hbase_prep = df_hbase.select(
    col("operation"),
    col("time_avg_ms").alias("hbase_time_ms"),
    col("time_min_ms").alias("hbase_time_min_ms"),
    col("time_max_ms").alias("hbase_time_max_ms"),
    col("rows_processed").alias("hbase_rows")
)

df_parquet_prep = df_parquet.select(
    col("operation"),
    col("time_avg_ms").alias("parquet_time_ms"),
    col("time_min_ms").alias("parquet_time_min_ms"),
    col("time_max_ms").alias("parquet_time_max_ms"),
    col("rows_processed").alias("parquet_rows"),
    col("size_mb").alias("parquet_size_mb")
)

# Joindre sur l'opération
df_comparison = df_hbase_prep.join(
    df_parquet_prep,
    on="operation",
    how="outer"
).orderBy("operation")

# Ajouter des colonnes calculées
df_comparison = df_comparison.withColumn(
    "speedup",
    when(col("hbase_time_ms") > 0, 
         spark_round(col("hbase_time_ms") / col("parquet_time_ms"), 2))
    .otherwise(None)
).withColumn(
    "time_diff_ms",
    col("hbase_time_ms") - col("parquet_time_ms")
).withColumn(
    "time_diff_percent",
    when(col("parquet_time_ms") > 0,
         spark_round((col("hbase_time_ms") - col("parquet_time_ms")) / col("parquet_time_ms") * 100, 2))
    .otherwise(None)
)

print("✓ Données préparées")
df_comparison.show(truncate=False)

# ============================================================
# 3. SAUVEGARDE DANS HIVE
# ============================================================
print("\n3. Sauvegarde dans Hive...")

df_comparison.write.mode("overwrite").saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE_COMPARISON}")

print(f"✓ Table Hive créée: {HIVE_DATABASE}.{HIVE_TABLE_COMPARISON}")

# ============================================================
# 4. EXPORT CSV
# ============================================================
print("\n4. Export CSV...")

import csv

# Convertir en liste de lignes pour export CSV (petit dataset)
rows = df_comparison.collect()
headers = df_comparison.columns

with open(COMPARISON_CSV, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)

print(f"✓ CSV exporté: {COMPARISON_CSV}")

# ============================================================
# 5. GÉNÉRATION DU RAPPORT MARKDOWN
# ============================================================
print("\n5. Génération du rapport markdown...")

# Collecter les données pour le rapport
comparison_data = df_comparison.collect()

# Générer le rapport
report_lines = []
report_lines.append("# RAPPORT DE BENCHMARK: PARQUET vs HBASE\n")
report_lines.append(f"**Date de génération:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
report_lines.append(f"**Environnement:** Docker Compose (Hadoop, Hive, HBase, Spark)\n\n")

report_lines.append("## RÉSUMÉ EXÉCUTIF\n\n")
report_lines.append("Ce rapport compare les performances entre Parquet et HBase pour différentes opérations:\n")
report_lines.append("- Lecture de données\n")
report_lines.append("- Écriture de données\n")
report_lines.append("- Requêtes Spark SQL (SELECT, FILTER, GROUP BY, COUNT)\n\n")

report_lines.append("## TABLEAU COMPARATIF\n\n")
report_lines.append("| Opération | HBase (ms) | Parquet (ms) | Différence (ms) | Speedup |\n")
report_lines.append("|-----------|------------|--------------|-----------------|----------|\n")

for row in comparison_data:
    operation = row['operation']
    hbase_time = row['hbase_time_ms'] if row['hbase_time_ms'] else 0
    parquet_time = row['parquet_time_ms'] if row['parquet_time_ms'] else 0
    time_diff = row['time_diff_ms'] if row['time_diff_ms'] else 0
    speedup = row['speedup'] if row['speedup'] else "N/A"
    
    report_lines.append(f"| {operation} | {hbase_time:.2f} | {parquet_time:.2f} | {time_diff:.2f} | {speedup} |\n")

report_lines.append("\n## ANALYSE DÉTAILLÉE\n\n")

# Analyser chaque opération
for row in comparison_data:
    operation = row['operation']
    hbase_time = row['hbase_time_ms'] if row['hbase_time_ms'] else 0
    parquet_time = row['parquet_time_ms'] if row['parquet_time_ms'] else 0
    speedup = row['speedup'] if row['speedup'] else None
    
    report_lines.append(f"### {operation.upper()}\n\n")
    report_lines.append(f"- **HBase:** {hbase_time:.2f} ms (min: {row['hbase_time_min_ms']:.2f}, max: {row['hbase_time_max_ms']:.2f})\n")
    report_lines.append(f"- **Parquet:** {parquet_time:.2f} ms (min: {row['parquet_time_min_ms']:.2f}, max: {row['parquet_time_max_ms']:.2f})\n")
    
    if speedup and speedup != "N/A":
        if speedup > 1:
            report_lines.append(f"- **Parquet est {speedup:.2f}x plus rapide que HBase**\n\n")
        elif speedup < 1:
            report_lines.append(f"- **HBase est {1/speedup:.2f}x plus rapide que Parquet**\n\n")
        else:
            report_lines.append(f"- **Performances équivalentes**\n\n")
    else:
        report_lines.append(f"- **Comparaison non disponible**\n\n")

# Informations supplémentaires sur Parquet
parquet_size_row = df_parquet.filter(col("operation") == "write").first()
if parquet_size_row:
    report_lines.append("## INFORMATIONS PARQUET\n\n")
    report_lines.append(f"- **Taille totale:** {parquet_size_row['size_mb']:.4f} MB\n")
    report_lines.append(f"- **Compression:** {parquet_size_row['compression']}\n")
    report_lines.append(f"- **Emplacement:** {PARQUET_HDFS_DIR}\n\n")

# Conclusion
report_lines.append("## CONCLUSION\n\n")
report_lines.append("### Points clés:\n\n")
report_lines.append("1. **Parquet** est généralement plus rapide pour les requêtes analytiques (SELECT, GROUP BY)\n")
report_lines.append("2. **HBase** peut être plus adapté pour les accès par clé (rowkey)\n")
report_lines.append("3. **Parquet** offre une meilleure compression et un stockage plus efficace\n")
report_lines.append("4. **HBase** nécessite plus de configuration et de maintenance\n\n")

report_lines.append("### Recommandations:\n\n")
report_lines.append("- Utiliser **Parquet** pour les workloads analytiques et les requêtes SQL complexes\n")
report_lines.append("- Utiliser **HBase** pour les accès en temps réel par clé et les mises à jour fréquentes\n")
report_lines.append("- Considérer une architecture hybride selon les besoins\n\n")

# Écrire le rapport
with open(COMPARISON_REPORT, 'w', encoding='utf-8') as f:
    f.writelines(report_lines)

print(f"✓ Rapport généré: {COMPARISON_REPORT}")

# ============================================================
# 6. AFFICHAGE DU RÉSUMÉ
# ============================================================
print("\n" + "=" * 60)
print("RÉSUMÉ DE LA COMPARAISON")
print("=" * 60)
df_comparison.select(
    "operation",
    "hbase_time_ms",
    "parquet_time_ms",
    "speedup",
    "time_diff_percent"
).show(truncate=False)

print(f"\n✓ Fichiers générés:")
print(f"  - CSV: {COMPARISON_CSV}")
print(f"  - Rapport: {COMPARISON_REPORT}")
print(f"  - Table Hive: {HIVE_DATABASE}.{HIVE_TABLE_COMPARISON}")

spark.stop()
print("\n✓ Génération du tableau comparatif terminée")

