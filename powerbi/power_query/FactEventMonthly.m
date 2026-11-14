// ============================================================================
// Power Query M Script: FactEventMonthly
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.v_event_monthly (Monthly aggregate serving view)
// Invariants:
// 1. Pre-aggregated grain: (year_month_jst, region_key, magnitude_band, depth_band, coverage)
// 2. Additive consistency: sum(event_count) strictly equals daily totals.
// 3. Weighted averages preserved across rollups.
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    TargetView = CatalogName & "." & SchemaName & ".v_event_monthly",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            year_month_jst,
            region_key,
            region_name,
            region_category,
            magnitude_band_code,
            depth_band_code,
            source_coverage_code,
            event_count,
            magnitude_non_null_count,
            magnitude_sum,
            magnitude_avg,
            magnitude_max,
            depth_non_null_count,
            depth_sum,
            depth_avg,
            strong_earthquake_count,
            tsunami_flagged_count,
            alerted_event_count,
            intensity_reported_count,
            offshore_or_unknown_count
        FROM " & TargetView & "
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"year_month_jst", Int64.Type},
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text},
        {"magnitude_band_code", type text},
        {"depth_band_code", type text},
        {"source_coverage_code", type text},
        {"event_count", Int64.Type},
        {"magnitude_non_null_count", Int64.Type},
        {"magnitude_sum", type number},
        {"magnitude_avg", type number},
        {"magnitude_max", type number},
        {"depth_non_null_count", Int64.Type},
        {"depth_sum", type number},
        {"depth_avg", type number},
        {"strong_earthquake_count", Int64.Type},
        {"tsunami_flagged_count", Int64.Type},
        {"alerted_event_count", Int64.Type},
        {"intensity_reported_count", Int64.Type},
        {"offshore_or_unknown_count", Int64.Type}
    })
in
    TypedTable
