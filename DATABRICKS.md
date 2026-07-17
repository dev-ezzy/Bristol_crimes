# Running This Project on Databricks Free Edition

Use this route if the Fabric trial is unavailable to you (it usually requires a work or school Microsoft account). Databricks Free Edition is genuinely free, needs no card, and accepts personal email addresses. The notebooks in this project run on both platforms unchanged thanks to the platform shim at the top of each one.

## 1. Sign up

Go to databricks.com/learn/free-edition (or signup.databricks.com and pick Free Edition, not the Express trial). Sign up with any email or a Google account. You get a serverless workspace immediately, no cluster setup needed.

## 2. Create the Volume for raw files

Databricks separates tables (Unity Catalog) from files (Volumes). Our Bronze zone is files, so:

1. In the left sidebar click Catalog.
2. Expand the `workspace` catalog, then the `default` schema.
3. Click Create, then Volume. Name it `bristol`, type Managed.
4. Inside the volume, click Upload and create a `config` folder, then upload `pipeline_config.json` into it.

The notebooks expect exactly this layout: `/Volumes/workspace/default/bristol/config/pipeline_config.json`. If you name things differently, update the `CONFIG_CANDIDATES` path at the top of each notebook and the `databricks` block in the config.

## 3. Import and run the notebooks

1. Workspace, Create, Notebook. Create four notebooks and paste in the code from `notebooks/` (or use File, Import and upload the .py files directly, Databricks converts them).
2. Each notebook auto-attaches to serverless compute on Free Edition. Nothing to configure.
3. Run in order: 01, 02, 04, 03. The shim prints `Running on: databricks` as its first output, a quick confirmation everything is wired right.
4. After 02 runs, check Catalog: `workspace.default.silver_crime` should exist as a managed Delta table. After 03, the four gold tables appear alongside it.

## 4. Orchestration (replaces Fabric Pipelines)

Databricks calls this Jobs (under Lakeflow in the sidebar):

1. Click Jobs & Pipelines, Create Job. Name it `bristol_crime_daily`.
2. Add four tasks, one per notebook, each depending on the previous: Bronze, then Silver, then DQ Gate, then Gold.
3. On the job's Schedule panel set a daily trigger, e.g. 06:00 Europe/London.
4. Under Notifications add your email on failure. Notebook 04 raising an exception fails the task, which fires the notification, the same failure-alert pattern as the Fabric version.

## 5. Dashboards (replaces Power BI, mostly)

Free Edition includes AI/BI Dashboards, Databricks' native BI tool:

1. Sidebar, Dashboards, Create Dashboard.
2. Add datasets pointing at your gold tables (or write SQL against them directly, the queries in `sql/gold_views.sql` translate almost verbatim, drop the `GO` statements and `CREATE OR ALTER VIEW` wrappers and just run the SELECTs).
3. Build the same layout as the Power BI guide: KPI counters, a line chart of monthly trend, a bar chart by area, and a map visual using the latitude and longitude columns.

If you specifically want Power BI on your CV for the Bristol role: install Power BI Desktop (free) and connect via the Databricks connector (Get Data, search Databricks). You'll need your workspace URL and a personal access token from Settings, Developer. Free Edition has some connectivity limits, so treat this as a bonus rather than the plan, the native dashboard demonstrates the same BI modelling skills.

## 6. How to frame this on your application

Do not present Databricks as a compromise, because it isn't one. Fabric's data engineering experience is Spark notebooks plus Delta Lake plus scheduled pipelines, which is exactly what you have built here. Your cover letter line writes itself: "I built an end to end medallion architecture pipeline (PySpark, Delta Lake, scheduled orchestration, data quality gates) ingesting Bristol's street level crime data, designed to be platform portable and verified against both Databricks and the Fabric API surface." Portability is a senior-sounding word because it reflects senior-sounding thinking.

## Differences cheat sheet

| Concept            | Fabric                       | Databricks Free Edition          |
|--------------------|------------------------------|----------------------------------|
| File storage       | Lakehouse Files/             | Unity Catalog Volumes            |
| Tables             | Lakehouse Tables             | Unity Catalog managed tables     |
| Utilities          | mssparkutils                 | dbutils (same .fs API)           |
| Orchestration      | Data Pipelines               | Lakeflow Jobs                    |
| SQL layer          | SQL analytics endpoint       | SQL editor / SQL warehouse       |
| BI                 | Power BI (DirectLake)        | AI/BI Dashboards                 |
| Compute            | Capacity based               | Serverless (quota limited)       |
