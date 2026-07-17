# Databricks notebook source
# runs between Silver and Gold
# Runs assertion checks against the Silver table. If any check fails, the
# notebook raises an exception, which fails the pipeline run, which stops
# Gold from being rebuilt from bad data and triggers your alert.

import json
from datetime import datetime, date
from pyspark.sql import functions as F

# path configuration

try:
    fsu = mssparkutils
    PLATFORM = "fabric"
except NameError:
    fsu = dbutils
    PLATFORM = "databricks"

CONFIG_CANDIDATES = {
    "fabric":     "Files/config/pipeline_config.json",
    "databricks": "/Volumes/workspace/default/bristol/config/pipeline_config.json",
}
raw_cfg = fsu.fs.head(CONFIG_CANDIDATES[PLATFORM], 1024 * 100)
cfg = json.loads(raw_cfg)

SILVER_TABLE = cfg["storage"]["silver_table"]
DQ           = cfg["data_quality"]
LAG          = cfg["ingestion"]["publication_lag_months"]

silver = spark.table(SILVER_TABLE)

# We collect results instead of failing on the first problem.

results = []   # list of dicts: {check, passed, detail}

def record(check_name: str, passed: bool, detail: str):
    """Log a check result to console and the results list."""
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {check_name}: {detail}")
    results.append({
        "check_name": check_name,
        "passed": passed,
        "detail": detail,
        "run_at": datetime.now().isoformat(),
    })


# check volume

min_rows = DQ["min_rows_per_month"]
monthly_counts = (silver.groupBy("crime_month").count().collect())

for row in monthly_counts:
    record(
        f"volume:{row['crime_month']}",
        row["count"] >= min_rows,
        f"{row['count']} rows (minimum {min_rows})",
    )

# check nullness

total = silver.count()

null_cat = silver.filter(F.col("crime_category").isNull()).count()
record("nullness:category",
       (null_cat / total) <= DQ["max_null_rate_category"],
       f"{null_cat}/{total} null categories (max rate {DQ['max_null_rate_category']})")

null_loc = silver.filter(F.col("latitude").isNull() | F.col("longitude").isNull()).count()
record("nullness:location",
       (null_loc / total) <= DQ["max_null_rate_location"],
       f"{null_loc}/{total} null locations (max rate {DQ['max_null_rate_location']})")


# validate the data

known = set(DQ["expected_categories"])
found = {r["crime_category"] for r in silver.select("crime_category").distinct().collect()}
unknown = found - known
record("validity:categories",
       len(unknown) == 0,
       f"unknown categories: {sorted(unknown) if unknown else 'none'}")

out_of_bounds = silver.filter(
    F.col("latitude").isNotNull() &
    (~F.col("latitude").between(51.3, 51.6) | ~F.col("longitude").between(-2.8, -2.4))
).count()
record("validity:coordinates_in_bristol",
       out_of_bounds == 0,
       f"{out_of_bounds} records outside Bristol bounding box")

# freshness of data
newest = silver.agg(F.max("crime_month")).collect()[0][0]
today = date.today()
grace = LAG + 1
exp_year, exp_month = today.year, today.month - grace
while exp_month <= 0:
    exp_month += 12
    exp_year -= 1
expected_min = f"{exp_year}-{exp_month:02d}"

record("freshness:newest_month",
       newest >= expected_min,
       f"newest month {newest} (expected >= {expected_min})")


# Results go to a Delta log table.(append mode) Then  we raise if anything failed. Order matters: log first,
# so failed runs are visible in the log too.

spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable("dq_check_log")

failures = [r for r in results if not r["passed"]]
if failures:
    raise RuntimeError(
        f"DATA QUALITY GATE FAILED: {len(failures)} of {len(results)} checks failed. "
        f"Failed checks: {[f['check_name'] for f in failures]}. "
        "Gold layer will NOT be rebuilt. See dq_check_log table for history."
    )

print(f"\nAll {len(results)} quality checks passed. Safe to build Gold.")