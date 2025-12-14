# BigData Project — Benchmarks (Parquet vs HBase) + Spark ML

**Purpose:** This repository provides an end-to-end benchmark and tutorial comparing storage formats (Parquet, ORC, Avro, CSV) and HBase when used with Spark and Hive, plus a Spark ML benchmarking pipeline that evaluates model performance and stores results in Hive.

**What it does (high-level):**
- Ingest CSV datasets from HDFS and register Hive external tables.
- Benchmark write/read times and sizes for CSV, Parquet, ORC, Avro and save metrics to Hive.
- Benchmark Parquet file performance (write/read, SQL queries) and save metrics to Hive.
- Benchmark HBase (write/scan, convert to Spark DataFrame, Spark SQL queries) and save metrics to Hive.
- Generate a comparative table and report (Parquet vs HBase) and export CSV/report files.
- Run Spark ML experiments on the dataset (multiple models, CV) and store ML metrics in Hive.

**Key files & folders:**
- `benchmarks/` — benchmark scripts:
  - `benchmark_formats.py` — multi-format benchmark (csv/parquet/orc/avro), saves `perf.format_benchmark_results`.
  - `benchmark_parquet_complete.py` — Parquet-specific benchmark, saves `perf.parquet_metrics`.
  - `benchmark_hbase.py` — HBase benchmark and Spark SQL queries, saves `perf.hbase_metrics`.
  - `generate_comparison.py` — joins HBase & Parquet metrics, writes `perf.comparison_results`, CSV and markdown report.
  - `read_csv_hive.py` — simple demo: read CSV → create external Hive table → write parquet/orc/avro.
  - `benchmark_config.py` — central configuration for HDFS/HBase/Hive paths and parameters.
- `spark_ml/` — ML benchmarking pipeline:
  - `ml_benchmark.py` — trains multiple models (LR, RF, GBT) with CV, computes metrics and saves to `perf.ml_benchmark_results`.
- `scripts/` — helper orchestration scripts (bash) for Docker-based execution:
  - `run_format_benchmark.sh`, `run_full_benchmark.sh` — orchestrate end-to-end runs.
  - `create_hbase_catalog.py` — generate HBase catalog JSON for connectors.
- `data/` — sample CSVs: `sample_sales.csv`, `large_sales.csv` (generator available in `scripts/generate_data.py`).
- `docs/` — documentation and tutorial files (we added `Tutoel_report.md`).

**Hive tables created by the pipelines:**
- `perf.format_benchmark_results` — results from `benchmark_formats.py`.
- `perf.parquet_metrics` — Parquet benchmark metrics.
- `perf.hbase_metrics` — HBase benchmark metrics.
- `perf.comparison_results` — joined Parquet vs HBase comparison (from `generate_comparison.py`).
- `perf.ml_benchmark_results` — ML experiment results (model metrics) from `spark_ml/ml_benchmark.py`.
- `perf.ml_input_summary` — input dataset summary saved by ML pipeline.

**Quick architecture overview (ASCII):**

```
+-----------------+       +-----------------+        +-----------------+
|  Docker Compose |------>|  HDFS (namenode)| <----->|  Data (CSV)     |
|  (Spark, Hive,  |       +-----------------+        +-----------------+
|   HBase, ZK)    |              ^
+-----------------+              |
       |                         |
       v                         v
+-----------------+       +-----------------+
| Spark (driver)  |  <--> | Hive Metastore  |
| - spark-submit  |       | - tables in HDFS|
| - run benchmarks|       +-----------------+
+-----------------+
       |
       v
+---------------------+
| HBase (for KV ops)  |
+---------------------+

Outputs: Hive tables in `perf.*`, local CSV/MD reports under `benchmark_results/` (if present).
```

**Prerequisites / Notes before running**
- This project expects a Docker Compose environment with containers referenced by scripts (e.g., `spark-master-new`, `namenode`, `hbase-master`, `zookeeper`). Adjust container names if different.
- Ensure Spark's `spark-avro` (or matching package) is available when running Avro operations. The scripts set `--packages` where needed but verify versions match your Spark version.
- HBase client jars must be available on Spark's classpath for `benchmark_hbase.py` to work; scripts try to pass packages and `hbase-site.xml`.
- `BENCHMARK_ITERATIONS` default is 1; increase to get meaningful min/max/avg values.
- If you run ML profiling (`--profile-html`), install `ydata_profiling` in the executor/driver environment.

**How to run (example commands)**
- Quick small-format benchmark (inside the `spark` container):

```bash
# run on spark container (example from scripts)
/spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  --packages org.apache.spark:spark-avro_2.12:3.1.1 \
  /benchmark_formats.py \
  --input-file "hdfs://namenode:8020/user/data/sample_sales.csv" \
  --dataset-name "small"
```

- Full end-to-end orchestrated (using provided script):

```bash
# on host (requires Docker Compose environment matching container names)
bash scripts/run_full_benchmark.sh
```

- Run the ML benchmark:

```bash
/spark/bin/spark-submit \
  --master spark://spark-master-new:7077 \
  spark_ml/ml_benchmark.py \
  --table perf.sales_parquet \
  --dataset sales \
  --sample-frac 0.5
```

**Where results live**
- Hive tables: `perf.*` (see list above). Use `spark-sql` or Hive CLI to query.
- Local CSV & Markdown: `benchmark_results/comparison_results.csv`, `benchmark_results/benchmark_report.md` (when generated by `generate_comparison.py`).

**Next steps / suggestions**
- Update `benchmark_config.py` to match your production HDFS/Hive/HBase addresses and increase `BENCHMARK_ITERATIONS`.
- Add a proper architecture diagram file `docs/architecture.png` and replace the ASCII block.
- Run the `run_full_benchmark.sh` workflow on the target Docker Compose setup and verify Hive tables after completion.

---

If you want, I can now run a quick consistency check of the Hive table creation SQL (list the create statements), or add a simple `docs/architecture.png` placeholder SVG. Which do you prefer next?