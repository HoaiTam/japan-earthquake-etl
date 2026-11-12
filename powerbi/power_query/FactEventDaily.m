// ============================================================================
// Power Query M Script: FactEventDaily
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.v_event_daily (Daily aggregate serving view)
// Invariants:
// 1. Pre-aggregated grain: (event_date_key_jst, region_key, magnitude_band, depth_band, coverage)
// 2. Exact fact parity: sum(event_count) matches distinct canonical events.
// 3. Honest NULL preservation: magnitude_avg remains null when non-null count is 0.
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            event_date_key_jst,
            event_date_jst,
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
        FROM iceberg.gold.v_event_daily
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"event_date_key_jst", Int64.Type},
        {"event_date_jst", type date},
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
