# Power BI Dashboard Setup

The final layer, and the one a hiring manager will actually look at. Budget 2 to 3 hours.

## 1. Create the semantic model (DirectLake)

1. Open your Lakehouse in Fabric, click New semantic model.
2. Select the four Gold tables: `gold_fact_crime`, `gold_dim_date`, `gold_dim_category`, `gold_crime_monthly_by_area`. Name it `Bristol Crime Semantic Model`.
3. Open the model view and create relationships (drag column to column):
   - `gold_fact_crime[month_key]` to `gold_dim_date[month_key]`, many to one, single direction
   - `gold_fact_crime[category_key]` to `gold_dim_category[category_key]`, many to one
   - `gold_crime_monthly_by_area[month_key]` to `gold_dim_date[month_key]`
   - `gold_crime_monthly_by_area[category_key]` to `gold_dim_category[category_key]`
4. On `gold_dim_date`, select `month_short` and set Sort by column to `month_sort`. This is the step everyone forgets, without it months sort alphabetically.

DirectLake means Power BI reads the Delta files directly, no import, no refresh schedule to manage, data is current the moment the pipeline finishes. This is Fabric's headline feature and worth naming explicitly in your README.

## 2. DAX measures

Create these on the fact table (New measure):

```dax
Total Crimes = COUNTROWS(gold_fact_crime)
```

```dax
Crimes Previous Month =
CALCULATE(
    [Total Crimes],
    DATEADD(gold_dim_date[month_date], -1, MONTH)
)
```

```dax
Month on Month Change % =
DIVIDE(
    [Total Crimes] - [Crimes Previous Month],
    [Crimes Previous Month]
)
```

```dax
Resolved Rate % =
DIVIDE(
    CALCULATE([Total Crimes],
        NOT gold_fact_crime[outcome] IN {
            "No outcome recorded",
            "Under investigation",
            "Status update unavailable"
        }),
    [Total Crimes]
)
```

Learning note: measures compute at query time in whatever filter context the visual provides. The same `Total Crimes` measure returns city totals on a card and per category values in a bar chart. That reusability is why measures beat calculated columns for aggregation.

## 3. Report layout (one page is enough)

**Top row, four KPI cards:** Total Crimes (latest month), Month on Month Change %, Resolved Rate %, and top category name.

**Left, main visual, map:** Azure Map or the built in Map visual. Latitude and longitude from the fact table, bubble size by count, legend by `category_group`. This is the visual that makes people stop scrolling.

**Right top, line chart:** `month_short` on the x axis, `Total Crimes` on y, legend by `category_group`. The trend story.

**Right bottom, bar chart:** crimes by `area`, latest month, drill down to `category_name`.

**Slicers:** month range and category group, placed top right.

## 4. Publish and share

Save the report to your workspace. For the portfolio, either publish to web (Settings, if allowed on your tenant) or record a 2 minute walkthrough with Loom and link it in the GitHub README. The video is often the stronger option: you control the narrative and it proves the pipeline actually runs.

## 5. Honest limitations (put these in your README, they add credibility)

- Locations are anonymised snap points, fine for area level analysis, wrong for address level claims.
- The `area` column comes from which query circle ingested the record, an approximation. A production version would spatially join coordinates to official ward boundaries (Bristol publishes ward GeoJSON on its open data portal), and naming that as your next step shows roadmap thinking.
- Recent months under report outcomes because investigations take time. Any outcome analysis should exclude the latest 3 months or say so.
