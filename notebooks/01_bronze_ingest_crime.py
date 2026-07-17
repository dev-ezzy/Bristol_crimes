# Databricks notebook source
# Pulls monthly street level crime data for Bristol from the UK Police API 
# and lands it in the Bronze zone of the lakehouse as raw JSON files,
# exactly as received, partitioned by crime month and query point.

import json
import time
import requests
from datetime import date

try:
    fsu = mssparkutils          # Microsoft Fabric / Synapse
    PLATFORM = "fabric"
except NameError:
    fsu = dbutils               # Databricks
    PLATFORM = "databricks"
print(f"Running on: {PLATFORM}")

# Config lives in a different place per platform (Fabric Lakehouse Files vs
# Databricks Unity Catalog Volume), so we hardcode ONLY the two candidate
# config locations and read everything else from the config itself.
CONFIG_CANDIDATES = {
    "fabric":     "Files/config/pipeline_config.json",
    "databricks": "/Volumes/workspace/default/bristol/config/pipeline_config.json",
}
raw_cfg = fsu.fs.head(CONFIG_CANDIDATES[PLATFORM], 1024 * 100)

cfg = json.loads(raw_cfg)

API_URL       = cfg["api"]["base_url"]
TIMEOUT       = cfg["api"]["request_timeout_seconds"]
MAX_RETRIES   = cfg["api"]["max_retries"]
BACKOFF       = cfg["api"]["retry_backoff_seconds"]
QUERY_POINTS  = cfg["bristol"]["query_points"]
BRONZE_PATH   = cfg["storage"][PLATFORM]["bronze_path"]
BACKFILL      = cfg["ingestion"]["backfill_months"]
LAG           = cfg["ingestion"]["publication_lag_months"]


# generating the month list in code rather than hardcoding
# dates is what lets this same notebook serve both the initial backfill AND
# the scheduled monthly incremental run without modification.

def month_list(n_months: int, lag: int) -> list[str]:
    """Return the last n_months in YYYY-MM format, starting `lag` months back.

    Example (today = 2026-07, lag=2, n=3) -> ['2026-05', '2026-04', '2026-03']
    """
    today = date.today()
    months = []
    # Start from (current month - lag) and walk backwards
    year, month = today.year, today.month - lag
    for _ in range(n_months):
        # Handle year rollover: month 0 becomes December of previous year
        while month <= 0:
            month += 12
            year -= 1
        months.append(f"{year}-{month:02d}")
        month -= 1
    return months


# Detect first run vs incremental: if Bronze already has data, only fetch
# the single latest month. Otherwise backfill the full window.
def bronze_is_empty(path: str) -> bool:
    try:
        return len(fsu.fs.ls(path)) == 0
    except Exception:
        return True  # path doesn't exist yet -> definitely first run

months_to_fetch = month_list(BACKFILL if bronze_is_empty(BRONZE_PATH) else 1, LAG)
print(f"Months to fetch: {months_to_fetch}")

# fetch with retry logic

def fetch_crimes(lat: float, lng: float, month: str) -> list[dict]:
    """Fetch all crimes within 1 mile of (lat, lng) for a given month."""
    params = {"lat": lat, "lng": lng, "date": month}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            print(f"  attempt {attempt}: HTTP {resp.status_code}, retrying...")
        except requests.RequestException as e:
            print(f"  attempt {attempt}: {e}, retrying...")
        # Exponential backoff: wait 5s, then 10s, then 20s...
        time.sleep(BACKOFF * (2 ** (attempt - 1)))
    raise RuntimeError(f"API failed after {MAX_RETRIES} retries for {month} @ ({lat},{lng})")


# land raw json in bronxe , partitioned

# downstream Spark reads can then "prune" partitions — if Silver only needs
# to reprocess May 2026, Spark reads ONE folder instead of scanning
# everything. On big data this is the difference between seconds and hours.

#   we write instead of appending as writing the same month twice should produce identical state. That is
#   idempotency. Appending would duplicate crimes on every re-run.
total_records = 0

for month in months_to_fetch:
    for point in QUERY_POINTS:
        print(f"Fetching {month} for {point['name']}...")
        crimes = fetch_crimes(point["lat"], point["lng"], month)

        # Tag each record with lineage metadata BEFORE writing. Knowing where
        # and when every record came from is basic data governance — the job
        # spec's "ensure data is managed appropriately and compliantly".
        for c in crimes:
            c["_ingest_point"] = point["name"]
            c["_ingest_month"] = month
            c["_ingested_at"]  = date.today().isoformat()

        out_dir  = f"{BRONZE_PATH}/month={month}/point={point['name']}"
        out_file = f"{out_dir}/data.json"

        # fsu.fs.put writes a single file; overwrite=True = idempotent
        fsu.fs.put(out_file, json.dumps(crimes), overwrite=True)

        print(f"  landed {len(crimes)} records -> {out_file}")
        total_records += len(crimes)

        # small pause between calls
        time.sleep(1)

print(f"\nBronze ingestion complete. {total_records} raw records landed.")
print("NOTE: totals include overlap between query points (the 1 mile radii")
print("overlap by design so no area is missed")