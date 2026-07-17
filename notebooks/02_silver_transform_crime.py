# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# silver transform — clean, flatten, deduplicate

import json
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

# loading configuration

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

BRONZE_PATH  = cfg["storage"][PLATFORM]["bronze_path"]
SILVER_TABLE = cfg["storage"]["silver_table"]

# read bronze

# spark.read.json handles the nested structure automatically, inferring a
# struct schema. The wildcard path reads every month/point partition at once.

df_raw = spark.read.option("multiLine", True).json(f"{BRONZE_PATH}/month=*/point=*/data.json")

print(f"Raw records read from Bronze: {df_raw.count()}")
df_raw.printSchema()  # always inspect the inferred schema errors and surprises live here

# flatten and cast data from bronze

df_silver = (
    df_raw
    .select(
        F.col("id").cast(IntegerType()).alias("crime_id"),
        F.col("persistent_id").alias("persistent_id"),
        F.col("category").alias("crime_category"),
        F.col("month").alias("crime_month"),

        # location struct -> flat, correctly typed columns
        F.col("location.latitude").cast(DoubleType()).alias("latitude"),
        F.col("location.longitude").cast(DoubleType()).alias("longitude"),
        F.col("location.street.id").cast(IntegerType()).alias("street_id"),
        F.col("location.street.name").alias("street_name"),

        # nullable nested struct -> flat columns with explicit defaults
        F.coalesce(F.col("outcome_status.category"), F.lit("No outcome recorded")).alias("outcome"),
        F.col("outcome_status.date").alias("outcome_month"),

        # lineage columns we added at ingestion
        F.col("_ingest_point").alias("ingest_point"),
        F.col("_ingested_at").cast("date").alias("ingested_at"),
    )
    # Derived columns that make analysis easier later:
    .withColumn("crime_year",  F.substring("crime_month", 1, 4).cast(IntegerType()))
    .withColumn("crime_month_num", F.substring("crime_month", 6, 2).cast(IntegerType()))
)


# deduplicate

from pyspark.sql.window import Window

dedup_window = Window.partitionBy("crime_id").orderBy(F.col("ingest_point"))

df_silver = (
    df_silver
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Records after dedup: {df_silver.count()}")

# write to delta with merge (idempotent upsert)

if not spark.catalog.tableExists(SILVER_TABLE):
    (df_silver.write
        .format("delta")
        .partitionBy("crime_year")      # partition on a low-cardinality column
        .saveAsTable(SILVER_TABLE))
    print(f"Created Delta table {SILVER_TABLE}")
else:
    target = DeltaTable.forName(spark, SILVER_TABLE)
    (target.alias("t")
        .merge(df_silver.alias("s"), "t.crime_id = s.crime_id")
        .whenMatchedUpdateAll()         # outcome may have changed -> update
        .whenNotMatchedInsertAll()      # brand new crime -> insert
        .execute())
    print(f"Merged into {SILVER_TABLE}")

# Quick sanity check to eyeball in the notebook output
spark.sql(f"""
    SELECT crime_month, COUNT(*) AS crimes
    FROM {SILVER_TABLE}
    GROUP BY crime_month ORDER BY crime_month DESC
""").show()