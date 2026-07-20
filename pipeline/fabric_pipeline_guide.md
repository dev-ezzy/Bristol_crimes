# Wiring the Notebooks into a Scheduled Fabric Pipeline

This is the orchestration step, the part of the job spec that says "develop processes to automate data loads on an agreed schedule". It takes about 20 minutes in the Fabric UI.

## What you are building

```
┌──────────────┐   on      ┌──────────────┐   on      ┌──────────────┐   on      ┌──────────────┐
│ 01 Bronze    │──success─▶│ 02 Silver    │──success─▶│ 04 DQ Gate   │──success─▶│ 03 Gold      │
│ Ingest       │           │ Transform    │           │ Checks       │           │ Aggregate    │
└──────────────┘           └──────────────┘           └──────┬───────┘           └──────────────┘
                                                             │ on failure
                                                             ▼
                                                      ┌──────────────┐
                                                      │ Teams / mail │
                                                      │ alert        │
                                                      └──────────────┘
```

Note the deliberate ordering: the quality gate (04) sits between Silver and Gold. If checks fail, Gold is never rebuilt and the dashboard keeps showing the last known good data. That is far better than showing broken numbers.

## Step by step

1. In your Fabric workspace click New item, then Data pipeline. Name it `pl_bristol_crime_daily`.

2. Add four Notebook activities from the Activities ribbon. Point each at the corresponding notebook and set a clear name (`Bronze Ingest`, `Silver Transform`, `DQ Gate`, `Gold Aggregate`).

3. Chain them with On success connectors: drag from the green tick on each activity to the next one. Bronze to Silver to DQ to Gold.

4. Add an Office 365 Outlook activity (or Teams activity) and connect it to the DQ Gate's red cross (On failure) connector. Subject line suggestion: `ALERT: Bristol crime pipeline failed data quality checks`. Because notebook 04 raises an exception on any failed check, this path fires automatically.

5. Set the schedule. Click Schedule in the top ribbon. Police data updates monthly, but running daily costs almost nothing at this scale and demonstrates the scheduling pattern properly. Suggested: daily at 06:00 UK time. The ingestion notebook is idempotent, so re-running against unchanged source data is harmless, the MERGE in Silver simply finds nothing new.

6. Run it once manually (Run button) and watch the run view. Every activity should go green in sequence. Screenshot this for your GitHub README, a green end to end pipeline run is exactly the artefact that makes a portfolio project feel real.

## Things worth knowing (and saying in interviews)

**Why daily when the data is monthly?** Because you rarely know exactly when a monthly source publishes. A cheap daily poll that no-ops when nothing changed is simpler and more reliable than trying to guess the publication day. Idempotency is what makes this safe.

**What happens if the API is down at 06:00?** Notebook 01 retries three times with exponential backoff, then fails, which fails the run, which you would see in the run history. Tomorrow's run picks up automatically. No data is lost because the source retains history.

**Retry at the pipeline level too.** On each notebook activity's Settings tab you can set Retry = 1 with a 10 minute interval. Belt and braces: code level retries handle transient API blips, activity level retries handle transient Spark cluster issues.

**Monitoring.** Workspace > Monitor shows every historical run with durations. Mention that at council scale you would also push run metadata to a log table and alert on duration anomalies (a run taking 4x longer than usual is a leading indicator of trouble).

## Databricks equivalent

If you build this on Databricks instead: Workflows > Create job > add the four notebooks as tasks with the same dependency chain, set a cron schedule (`0 0 6 * * ?`), and configure an email notification on failure. Identical concept, different UI. Being able to say "I understand the orchestration pattern, the tool is a detail" is a strong interview position.
