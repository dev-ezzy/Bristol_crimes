# Databricks notebook source
# gold aggregate - star schema for power BI
# Builds the analytics-ready layer from Silver:
# gold_fact_crime          one row per crime, foreign keys to dims
# gold_dim_date            calendar dimension (month grain)
# gold_dim_category        crime category lookup with friendly names
# gold_crime_monthly_by_area   pre-aggregated summary for fast visuals

import json
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
FACT_TABLE   = cfg["storage"]["gold_fact_table"]
DIM_DATE     = cfg["storage"]["gold_dim_date"]
DIM_CATEGORY = cfg["storage"]["gold_dim_category"]
MONTHLY_AGG  = cfg["storage"]["gold_monthly_agg"]

silver = spark.table(SILVER_TABLE)

# date dimension (month grain)

# The police data is monthly, so our date dim is monthly too. We derive it
# FROM the data (distinct months present) plus useful attributes Power BI
# will use for sorting and display.

dim_date = (
    silver
    .select("crime_month").distinct()
    .withColumn("month_date",  F.to_date(F.concat(F.col("crime_month"), F.lit("-01"))))
    .withColumn("year",        F.year("month_date"))
    .withColumn("month_num",   F.month("month_date"))
    .withColumn("month_name",  F.date_format("month_date", "MMMM"))
    .withColumn("month_short", F.date_format("month_date", "MMM yy"))
    .withColumn("quarter",     F.concat(F.lit("Q"), F.quarter("month_date")))
    .withColumn("month_sort",  (F.col("year") * 100 + F.col("month_num")))
    .withColumnRenamed("crime_month", "month_key")
)

dim_date.write.format("delta").mode("overwrite").saveAsTable(DIM_DATE)
print(f"{DIM_DATE}: {dim_date.count()} months")

# category dimension
# The API uses slugs like "violent-crime". Dashboards should show "Violent
# crime". A dimension table is the right home for display names, groupings,
# and any future attributes (e.g. severity weighting).
# We also add a broader grouping — useful when 14 categories is too many
# lines for one chart.

category_groups = {
    "violent-crime":          "Violence",
    "possession-of-weapons":  "Violence",
    "robbery":                "Violence",
    "public-order":           "Violence",
    "burglary":               "Property",
    "vehicle-crime":          "Property",
    "bicycle-theft":          "Property",
    "shoplifting":            "Property",
    "other-theft":            "Property",
    "theft-from-the-person":  "Property",
    "criminal-damage-arson":  "Property",
    "drugs":                  "Other",
    "anti-social-behaviour":  "Other",
    "other-crime":            "Other",
}

# Build a small lookup DataFrame from the dict. createDataFrame from a list
# of tuples is the standard pattern for reference data this size.
dim_category = spark.createDataFrame(
    [(slug, slug.replace("-", " ").capitalize(), grp)
     for slug, grp in category_groups.items()],
    ["category_key", "category_name", "category_group"]
)

dim_category.write.format("delta").mode("overwrite").saveAsTable(DIM_CATEGORY)
print(f"{DIM_CATEGORY}: {dim_category.count()} categories")

# fact table
# One row per crime. We keep the columns thin: keys, measures-in-waiting,
# and the coordinates Power BI's map visual needs. Descriptive text lives
# in dimensions, not here.

fact_crime = (
    silver
    .select(
        "crime_id",
        F.col("crime_month").alias("month_key"),        # FK -> dim_date
        F.col("crime_category").alias("category_key"),  # FK -> dim_category
        "latitude",
        "longitude",
        "street_name",
        "outcome",
        F.col("ingest_point").alias("area"),            # rough area label
    )
)

fact_crime.write.format("delta").mode("overwrite").saveAsTable(FACT_TABLE)
print(f"{FACT_TABLE}: {fact_crime.count()} crimes")

# pre-aggregated monthly summary
# Power BI COULD compute this on the fly from the fact table, and for this
# data volume it would be fine. We build it anyway because:
#  a) it demonstrates the pattern (at council scale, pre-aggregation is
#     what keeps dashboards under the 2 second response bar), and
#  b)it gives the Fabric SQL endpoint something meaningful to serve to
#     analysts who just want a simple table 

monthly_agg = (
    fact_crime
    .groupBy("month_key", "area", "category_key")
    .agg(
        F.count("*").alias("crime_count"),
        F.countDistinct("street_name").alias("streets_affected"),
    )
)

monthly_agg.write.format("delta").mode("overwrite").saveAsTable(MONTHLY_AGG)
print(f"{MONTHLY_AGG}: {monthly_agg.count()} aggregate rows")

# eye ball check - top categories last month

spark.sql(f"""
    SELECT d.month_short, c.category_name, SUM(m.crime_count) AS crimes
    FROM {MONTHLY_AGG} m
    JOIN {DIM_DATE} d     ON m.month_key = d.month_key
    JOIN {DIM_CATEGORY} c ON m.category_key = c.category_key
    WHERE d.month_sort = (SELECT MAX(month_sort) FROM {DIM_DATE})
    GROUP BY d.month_short, c.category_name
    ORDER BY crimes DESC
    LIMIT 10
""").show(truncate=False)

print("Gold layer complete. Power BI connects to these four tables.")