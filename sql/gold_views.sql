-- ============================================================================
-- GOLD VIEWS for the Fabric SQL Analytics Endpoint
-- ============================================================================
-- Every Fabric Lakehouse automatically exposes a read-only SQL endpoint over
-- its Delta tables. These views give analysts (and Power BI, if you prefer
-- import mode over DirectLake) clean, joined, business-friendly datasets
-- without them needing to understand the star schema.
--
-- HOW TO RUN: in Fabric, open your Lakehouse, switch to "SQL analytics
-- endpoint" (top right dropdown), open a new SQL query, paste and run.
--
-- LEARNING NOTE: views vs tables. A view is a saved query — it computes at
-- read time, always reflecting current data, and costs no storage. Use views
-- for convenience layers like this; use tables (notebook 03) when the
-- computation is heavy enough that you don't want to repeat it per query.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- VIEW 1: Monthly trend by category group
-- Powers the headline "is crime going up or down?" line chart.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_crime_monthly_trend AS
SELECT
    d.month_date,
    d.month_short,
    d.month_sort,               -- Power BI: use as "Sort by column" on month_short
    c.category_group,
    SUM(m.crime_count) AS crimes
FROM gold_crime_monthly_by_area AS m
JOIN gold_dim_date     AS d ON m.month_key    = d.month_key
JOIN gold_dim_category AS c ON m.category_key = c.category_key
GROUP BY d.month_date, d.month_short, d.month_sort, c.category_group;
GO


-- ----------------------------------------------------------------------------
-- VIEW 2: Area league table, latest month
-- Powers the "which areas need attention right now?" bar chart.
-- The subquery finds the most recent month present in the data, so the view
-- never needs a hardcoded date — it self-updates as new months land.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_area_latest_month AS
SELECT
    m.area,
    c.category_name,
    SUM(m.crime_count)      AS crimes,
    MAX(d.month_short)      AS month_label
FROM gold_crime_monthly_by_area AS m
JOIN gold_dim_date     AS d ON m.month_key    = d.month_key
JOIN gold_dim_category AS c ON m.category_key = c.category_key
WHERE d.month_sort = (SELECT MAX(month_sort) FROM gold_dim_date)
GROUP BY m.area, c.category_name;
GO


-- ----------------------------------------------------------------------------
-- VIEW 3: Outcome funnel
-- What proportion of crimes end in each outcome? Interesting, slightly
-- sobering, and a good talking point about what the data can and cannot
-- tell you (outcomes update months after the crime, so recent months skew
-- heavily to "Under investigation" / "No outcome recorded").
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_outcome_summary AS
SELECT
    f.outcome,
    COUNT(*)                                            AS crimes,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                               AS pct_of_total
FROM gold_fact_crime AS f
GROUP BY f.outcome;
GO


-- ----------------------------------------------------------------------------
-- VIEW 4: Street hotspots (top repeat locations, last 3 months)
-- DENSE_RANK + a rolling window: two more SQL techniques worth being able
-- to explain. "On or near" prefixes come from the police anonymisation.
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_street_hotspots AS
WITH last_three AS (
    SELECT DISTINCT TOP 3 month_key, month_sort
    FROM gold_dim_date
    ORDER BY month_sort DESC
)
SELECT TOP 25
    f.street_name,
    f.area,
    COUNT(*) AS crimes,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS hotspot_rank
FROM gold_fact_crime AS f
JOIN last_three lt ON f.month_key = lt.month_key
WHERE f.street_name IS NOT NULL
GROUP BY f.street_name, f.area
ORDER BY crimes DESC;
GO
