# Bristol Crime & Safety Analytics Pipeline

![Bristol Crime & Safety dashboard](screenshots/dashboard.png)

An end to end data engineering project ingesting street level crime data for Bristol from the official UK Police open data API, transforming it through a medallion architecture (Bronze, Silver, Gold) with a data quality gate, and serving a published interactive dashboard. Built and running on Databricks (Free Edition, serverless); designed to be platform portable, with the same notebooks verified against the Azure Fabric API surface — the stack used by Bristol City Council's Data and Insight Team.

---

## Architecture

```
UK Police API (data.police.uk)
        │  REST, no auth, monthly street level crime JSON
        ▼
┌─────────────────────────────────────────────────┐
│  01_bronze_ingest_crime                         │
│  Raw JSON landed exactly as received            │
│  Partitioned by ingestion month, immutable      │
└─────────────────────────────────────────────────┘
        ▼
┌─────────────────────────────────────────────────┐
│  02_silver_transform_crime                      │
│  Flatten nested JSON, cast types, dedupe,       │
│  standardise categories → Delta table           │
└─────────────────────────────────────────────────┘
        ▼
┌─────────────────────────────────────────────────┐
│  04_data_quality_checks (gate)                  │
│  Row counts, null thresholds, schema checks     │
│  Pipeline fails loudly if quality drops         │
└─────────────────────────────────────────────────┘
        ▼
┌─────────────────────────────────────────────────┐
│  03_gold_aggregate_crime                        │
│  Star schema: fact_crime + dim_date +           │
│  dim_category + monthly ward aggregates         │
└─────────────────────────────────────────────────┘
        ▼
   AI/BI Dashboard (Databricks) · Power BI portable
```

## Data source

The UK Police API publishes anonymised street level crime for all forces in England and Wales, updated monthly. Bristol is covered by the Avon and Somerset force. Bristol City Council's own open data portal sources its street crime dataset from this same API, so you are literally working with the council's upstream source.

- Docs: https://data.police.uk/docs/
- Endpoint used: `GET https://data.police.uk/api/crimes-street/all-crime?lat={lat}&lng={lng}&date=YYYY-MM`
- No API key required. Open Government Licence.
- Note: locations are anonymised snap points, not exact addresses. This matters for how you talk about the data in interviews (privacy by design).

## Project layout

```
bristol-crime-pipeline/
├── README.md
├── requirements.txt
├── config/
│   └── pipeline_config.json      All tunable parameters in one place
├── notebooks/
│   ├── 01_bronze_ingest_crime.py
│   ├── 02_silver_transform_crime.py
│   ├── 03_gold_aggregate_crime.py
│   └── 04_data_quality_checks.py
├── sql/
│   └── gold_views.sql            T-SQL views for the Fabric SQL endpoint
├── pipeline/
│   └── fabric_pipeline_guide.md  Wiring notebooks into a scheduled pipeline
└── powerbi/
    └── dashboard_setup.md       AI/BI Dashboard (Databricks) · Power BI portable
```

## Quick start (Fabric)

1. Sign up for the free Microsoft Fabric trial at app.fabric.microsoft.com (60 days, no card needed).
2. Create a workspace, e.g. `bristol-analytics`.
3. Inside it create a Lakehouse named `bristol_lakehouse`. This gives you OneLake storage (Files for raw, Tables for Delta) automatically. No separate ADLS setup needed on the trial.
4. Create four notebooks and paste in the code from `notebooks/`, in order. Attach each to `bristol_lakehouse`.
5. Run `01` manually first. Confirm raw JSON appears under `Files/bronze/crime/`.
6. Run `02`, then `04`, then `03`. Confirm Delta tables appear under Tables.
7. Follow `pipeline/fabric_pipeline_guide.md` to chain them into a scheduled Data Pipeline.
8. Follow `powerbi/dashboard_setup.md` to build the dashboard.

## Quick start (Databricks, alternative)

The PySpark code is identical. Only the storage paths change: swap the Fabric Lakehouse paths in `config/pipeline_config.json` for DBFS or ADLS mount paths, and use Databricks Workflows instead of Fabric Pipelines for scheduling.

## Why medallion architecture

- **Bronze** is your insurance policy. If your transform logic has a bug, you re run from raw without hitting the API again.
- **Silver** is a single cleaned version of the truth that any downstream consumer can trust.
- **Gold** is shaped for the questions the business asks, so Power BI stays fast and simple.

This is the pattern Fabric is designed around.
