# -*- coding: utf-8 -*-
import time
import json
import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.functions import vector_to_array


# ----------------------------------------------------------------------
# Prétraitement & helpers
# ----------------------------------------------------------------------
def build_preprocessing_stages(
    categorical_cols,
    numeric_cols,
    label_col="label",
    features_col="features",
):
    """
    Construit les étapes de prétraitement :
    - StringIndexer + OneHotEncoder pour les colonnes catégorielles
    - VectorAssembler pour fusionner toutes les features
    - StandardScaler pour normaliser
    """
    indexers = [
        StringIndexer(
            inputCol=col,
            outputCol=f"{col}_idx",
            handleInvalid="keep",
        )
        for col in categorical_cols
    ]

    encoders = [
        OneHotEncoder(
            inputCol=f"{col}_idx",
            outputCol=f"{col}_ohe",
        )
        for col in categorical_cols
    ]

    assembler_inputs = [f"{col}_ohe" for col in categorical_cols] + numeric_cols

    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="features_raw",
        handleInvalid="keep",
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol=features_col,
        withStd=True,
        withMean=False,
    )

    stages = indexers + encoders + [assembler, scaler]
    return stages


def quantile_based_label(df, amount_col="amount", label_col="label", q=0.75):
    """
    Crée une colonne label binaire :
    label = 1 si amount >= quantile q, sinon 0
    Retourne df, valeur du quantile.
    """
    quantile_val = df.approxQuantile(amount_col, [q], 0.01)[0]
    df = df.withColumn(
        label_col,
        (F.col(amount_col) >= F.lit(quantile_val)).cast("int"),
    )
    return df, quantile_val


def compute_class_weights(df, label_col="label"):
    """
    Calcule des poids de classe inverses des fréquences :
    w_c = N / (K * n_c)
    (class weight balancing pour classification cost-sensitive)
    """
    counts = df.groupBy(label_col).count().collect()
    if not counts:
        return {}

    total = sum(r["count"] for r in counts)
    num_classes = len(counts)

    weights = {}
    for r in counts:
        lbl = r[label_col]
        cnt = r["count"]
        if cnt > 0:
            weights[lbl] = float(total) / (num_classes * float(cnt))
        else:
            weights[lbl] = 1.0

    return weights


def add_class_weight_column(
    df, label_col="label", weight_col="class_weight", class_weights=None
):
    """
    Ajoute une colonne de poids de classe à partir d'un dict {label: weight}.
    """
    if not class_weights:
        # Pas de pondération
        return df

    # Valeur par défaut si label inconnu
    default_w = 1.0
    expr = F.lit(float(default_w))

    # On empile des when(...) pour chaque label connu
    for lbl, w in class_weights.items():
        expr = F.when(F.col(label_col) == F.lit(lbl), F.lit(w)).otherwise(expr)

    return df.withColumn(weight_col, expr)


def temporal_train_test_split(df, timestamp_col="date_ts", train_ratio=0.8, seed=42):
    """
    Split temporel :
    - Convertit le timestamp en long (secondes depuis l'epoch)
    - Coupe au quantile `train_ratio`
    - Tout ce qui est avant -> train, après -> test
    """
    df = df.withColumn("date_unix", F.col(timestamp_col).cast("long"))
    cutoff = df.approxQuantile("date_unix", [train_ratio], 0.01)[0]

    train_df = df.filter(F.col("date_unix") <= F.lit(cutoff))
    test_df = df.filter(F.col("date_unix") > F.lit(cutoff))

    return train_df, test_df, cutoff


def compute_pr_auc(
    predictions, label_col="label", probability_col="probability", sample_frac=0.5
):
    """
    Calcule l'aire sous la courbe Precision-Recall (AUC PR) à partir
    des probabilités prédites. On peut échantillonner pour réduire le coût.

    Fixe le bug Spark : probability est un Vector (struct), on passe par vector_to_array.
    """
    preds = predictions
    if 0 < sample_frac < 1.0:
        preds = predictions.sample(withReplacement=False, fraction=sample_frac, seed=42)

    # probability : VectorUDT -> on le convertit en array<double> puis on prend la proba de la classe 1
    score_and_labels = (
        preds.select(
            vector_to_array(F.col(probability_col))[1].alias("score"),
            F.col(label_col).cast("float").alias("label"),
        )
        .rdd.map(lambda row: (row["score"], row["label"]))
    )

    metrics = BinaryClassificationMetrics(score_and_labels)
    return float(metrics.areaUnderPR)


def get_models(features_col="features", label_col="label", weight_col=None):
    """
    Définit les modèles + grilles d'hyperparamètres.
    Approche innovante : comparaison de plusieurs familles de modèles
    + CrossValidation.
    """
    models_and_grids = []

    # Arguments communs pour weightCol (classification cost-sensitive)
    lr_kwargs = {}
    rf_kwargs = {}
    gbt_kwargs = {}
    if weight_col:
        lr_kwargs["weightCol"] = weight_col
        rf_kwargs["weightCol"] = weight_col
        gbt_kwargs["weightCol"] = weight_col

    # 1) Régression Logistique
    lr = LogisticRegression(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=50,
        **lr_kwargs,
    )
    lr_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )
    models_and_grids.append(("logistic_regression", lr, lr_grid))

    # 2) Random Forest
    rf = RandomForestClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        seed=42,
        **rf_kwargs,
    )
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [5, 10])
        .build()
    )
    models_and_grids.append(("random_forest", rf, rf_grid))

    # 3) Gradient-Boosted Trees
    gbt = GBTClassifier(
        featuresCol=features_col,
        labelCol=label_col,
        maxIter=50,
        seed=42,
        **gbt_kwargs,
    )
    gbt_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [3, 5])
        .addGrid(gbt.stepSize, [0.05, 0.1])
        .build()
    )
    models_and_grids.append(("gbt_classifier", gbt, gbt_grid))

    return models_and_grids


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Spark ML benchmark avancé")
    parser.add_argument(
        "--table",
        type=str,
        default="perf.sales_parquet",
        help="Table Hive source contenant les transactions",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="sales",
        help="Nom logique du dataset (ex: small, huge, sales, ...)",
    )
    parser.add_argument(
        "--source-format",
        type=str,
        default="parquet",
        help="Format de stockage initial (csv, parquet, orc, avro) pour info",
    )
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=1.0,
        help="Fraction de données à sampler (0 < f <= 1.0) pour réduire la taille",
    )
    parser.add_argument(
        "--split-strategy",
        type=str,
        choices=["random", "temporal"],
        default="random",
        help="Stratégie de split train/test : random ou temporal (time-aware)",
    )
    parser.add_argument(
        "--label-quantile",
        type=float,
        default=0.75,
        help="Quantile utilisé pour définir le seuil du label (ex: 0.75)",
    )
    # --- Nouveau : options de profiling YData ---
    parser.add_argument(
        "--profile-html",
        type=str,
        default=None,
        help=(
            "Chemin du rapport HTML YData Profiling "
            "(ex: /spark/reports/sales_profile.html). "
            "Si None, pas de profilage."
        ),
    )
    parser.add_argument(
        "--profile-sample-rows",
        type=int,
        default=50000,
        help="Nombre max de lignes à extraire pour le profiling (collect côté driver).",
    )

    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. SparkSession avec support Hive
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder.appName("SparkMLBenchmarkAdvanced")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"=== Spark ML Benchmark (avancé) ===")
    print(f"Source table      : {args.table}")
    print(f"Dataset (logical) : {args.dataset}")
    print(f"Source format     : {args.source_format}")
    print(f"Sample fraction   : {args.sample_frac}")
    print(f"Split strategy    : {args.split_strategy}")
    print(f"Label quantile    : {args.label_quantile}")
    if args.profile_html:
        print(
            f"Profiling HTML    : {args.profile_html} "
            f"(sample rows = {args.profile_sample_rows})"
        )

    # ------------------------------------------------------------------
    # 2. Lecture des données depuis Hive
    # ------------------------------------------------------------------
    df = spark.table(args.table)

    # Optionnel : sample pour aller plus vite si dataset énorme
    if 0 < args.sample_frac < 1.0:
        df = df.sample(withReplacement=False, fraction=args.sample_frac, seed=42)

    # On suppose au minimum : id, product, amount, date
    required_cols = ["id", "product", "amount", "date"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"Colonne requise '{col}' absente de la table {args.table}. "
                f"Schéma disponible : {df.columns}"
            )

    # Nettoyage minimal : enlever les nulls sur les colonnes clés
    df = df.dropna(subset=["product", "amount", "date"])

    # ------------------------------------------------------------------
    # 2bis. Reporting de base côté Spark (stats d'entrée)
    #      -> table Hive perf.ml_input_summary
    # ------------------------------------------------------------------
    print("=== Reporting Spark des variables d'entrée ===")
    basic_summary = (
        df.select("amount")
        .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
        .withColumn("dataset", F.lit(args.dataset))
        .withColumn("source_format", F.lit(args.source_format))
        .select(
            "dataset",
            "source_format",
            F.col("summary").alias("stat"),
            F.col("amount").alias("amount_value"),
        )
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS perf")
    basic_summary.write.mode("append").format("parquet").saveAsTable(
        "perf.ml_input_summary"
    )
    print("✓ Stats d'entrée enregistrées dans perf.ml_input_summary")

    # ------------------------------------------------------------------
    # 2ter. YData Profiling (profiling avancé type article scientifique)
    # ------------------------------------------------------------------
    if args.profile_html:
        try:
            from ydata_profiling import ProfileReport

            # Sample pour limiter la taille en mémoire
            print("=== Génération du rapport YData Profiling (échantillon) ===")
            sample_df = df.limit(args.profile_sample_rows).toPandas()

            # Création du dossier de sortie si besoin
            out_dir = os.path.dirname(args.profile_html)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            profile = ProfileReport(
                sample_df,
                title=f"Data Profiling – {args.dataset} ({args.source_format})",
                explorative=True,
            )
            profile.to_file(args.profile_html)
            print(f"✓ Rapport YData Profiling généré : {args.profile_html}")
        except ImportError:
            print(
                "⚠️ ydata_profiling n'est pas installé dans le container. "
                "Installe-le avec : pip install ydata-profiling"
            )
        except Exception as e:
            print(f"⚠️ Erreur pendant la génération du rapport YData Profiling : {e}")

    # ------------------------------------------------------------------
    # 3. Feature engineering : date → month, day_of_week, etc.
    # ------------------------------------------------------------------
    # Attention au format de la date, adapter si nécessaire
    df = df.withColumn("date_ts", F.to_timestamp("date"))
    df = df.withColumn("month", F.month("date_ts"))
    df = df.withColumn("day_of_week", F.dayofweek("date_ts"))

    # ------------------------------------------------------------------
    # 4. Création du label binaire basé sur amount (quantile configurable)
    # ------------------------------------------------------------------
    df, threshold_amount = quantile_based_label(
        df, amount_col="amount", label_col="label", q=args.label_quantile
    )
    print(
        f"Seuil (quantile {args.label_quantile:.2f}) pour amount = "
        f"{threshold_amount:.4f}"
    )

    # ------------------------------------------------------------------
    # 5. Définition des colonnes features
    # ------------------------------------------------------------------
    categorical_cols = ["product"]
    numeric_cols = ["amount", "month", "day_of_week"]
    label_col = "label"
    features_col = "features"
    weight_col = "class_weight"

    preprocessing_stages = build_preprocessing_stages(
        categorical_cols=categorical_cols,
        numeric_cols=numeric_cols,
        label_col=label_col,
        features_col=features_col,
    )

    # ------------------------------------------------------------------
    # 6. Split train/test (random ou temporel)
    # ------------------------------------------------------------------
    if args.split_strategy == "temporal":
        train_df, test_df, cutoff = temporal_train_test_split(
            df, timestamp_col="date_ts", train_ratio=0.8
        )
        print(f"Temporal split cutoff (date_unix) = {cutoff}")
    else:
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    n_train = train_df.count()
    n_test = test_df.count()

    pos_train = train_df.filter(F.col(label_col) == 1).count()
    pos_test = test_df.filter(F.col(label_col) == 1).count()

    pos_ratio_train = (float(pos_train) / n_train) if n_train > 0 else 0.0
    pos_ratio_test = (float(pos_test) / n_test) if n_test > 0 else 0.0

    print(f"Train count: {n_train}, Test count: {n_test}")
    print(
        f"Ratio de positifs (label=1) - train: {pos_ratio_train:.3f}, "
        f"test: {pos_ratio_test:.3f}"
    )

    # ------------------------------------------------------------------
    # 7. Classification cost-sensitive : poids de classe
    # ------------------------------------------------------------------
    class_weights = compute_class_weights(train_df, label_col=label_col)
    print(f"Class weights (train) : {class_weights}")

    train_df = add_class_weight_column(
        train_df, label_col=label_col, weight_col=weight_col, class_weights=class_weights
    )
    # On applique les mêmes poids au test (même structure, même colonne)
    test_df = add_class_weight_column(
        test_df, label_col=label_col, weight_col=weight_col, class_weights=class_weights
    )

    # ------------------------------------------------------------------
    # 8. Définition des modèles + ParamGrid
    # ------------------------------------------------------------------
    models_and_grids = get_models(
        features_col=features_col,
        label_col=label_col,
        weight_col=weight_col,
    )

    # Evaluateurs
    binary_eval = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="accuracy",
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="f1",
    )
    precision_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedPrecision",
    )
    recall_eval = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedRecall",
    )

    results = []

    # ------------------------------------------------------------------
    # 9. Entraînement + évaluation pour chaque modèle
    # ------------------------------------------------------------------
    for model_name, estimator, param_grid in models_and_grids:
        print(f"\n>>> Training model: {model_name}")

        # Pipeline complet : preprocessing + modèle
        pipeline = Pipeline(
            stages=preprocessing_stages + [estimator],
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=binary_eval,
            numFolds=3,
            parallelism=2,
        )

        t0 = time.time()
        cv_model = cv.fit(train_df)
        train_time = time.time() - t0

        best_model = cv_model.bestModel  # PipelineModel
        t1 = time.time()
        predictions = best_model.transform(test_df)
        test_time = time.time() - t1

        auc_roc = binary_eval.evaluate(predictions)
        accuracy = acc_eval.evaluate(predictions)
        f1 = f1_eval.evaluate(predictions)
        precision = precision_eval.evaluate(predictions)
        recall = recall_eval.evaluate(predictions)
        auc_pr = compute_pr_auc(
            predictions, label_col=label_col, probability_col="probability"
        )

        # Récupération des hyperparamètres du dernier stage (le modèle)
        last_stage = best_model.stages[-1]
        param_map = {}
        for p in last_stage.params:
            try:
                # certains paramètres internes (ex: bounds) n'ont ni valeur ni défaut
                param_map[p.name] = last_stage.getOrDefault(p)
            except KeyError:
                # on les ignore simplement
                continue

        params_json = json.dumps(param_map)

        print(
            f"Model {model_name} – AUC ROC: {auc_roc:.4f}, AUC PR: {auc_pr:.4f}, "
            f"Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, "
            f"Recall: {recall:.4f}, F1: {f1:.4f}, "
            f"train_time: {train_time:.2f}s, test_time: {test_time:.2f}s"
        )

        results.append(
            (
                args.dataset,  # dataset
                args.source_format,  # source_format
                model_name,  # model
                float(train_time),  # train_time_s
                float(test_time),  # test_time_s
                float(auc_roc),  # auc_roc
                float(auc_pr),  # auc_pr
                float(accuracy),  # accuracy
                float(precision),  # precision
                float(recall),  # recall
                float(f1),  # f1
                int(n_train),  # n_train
                int(n_test),  # n_test
                float(pos_ratio_train),  # pos_ratio_train
                float(pos_ratio_test),  # pos_ratio_test
                args.split_strategy,  # split_strategy
                float(args.label_quantile),  # label_quantile
                float(threshold_amount),  # label_threshold_amount
                params_json,  # params_json
            )
        )

    # ------------------------------------------------------------------
    # 10. Sauvegarde des résultats dans Hive
    # ------------------------------------------------------------------
    schema_cols = [
        "dataset",
        "source_format",
        "model",
        "train_time_s",
        "test_time_s",
        "auc_roc",
        "auc_pr",
        "accuracy",
        "precision",
        "recall",
        "f1",
        "n_train",
        "n_test",
        "pos_ratio_train",
        "pos_ratio_test",
        "split_strategy",
        "label_quantile",
        "label_threshold_amount",
        "params_json",
    ]

    results_df = spark.createDataFrame(results, schema=schema_cols)

    # S'assurer que la DB existe
    spark.sql("CREATE DATABASE IF NOT EXISTS perf")

    # On écrase la table pour ce run (on peut passer en append selon les besoins)
    results_df.write.mode("overwrite").format("parquet").saveAsTable(
        "perf.ml_benchmark_results"
    )

    print("\n✓ ML benchmark results saved to perf.ml_benchmark_results")

    spark.stop()


if __name__ == "__main__":
    main()
