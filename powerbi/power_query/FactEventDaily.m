// ============================================================================
// Power Query M: FactEventDaily
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Daily earthquake aggregates calculated directly from the pinned publication bundle.
// Source: Immutable versioned serving view v_<bundle_suffix>_earthquake_event_current.
// Guarantee: Exactly consistent with FactEarthquakeEvents and ActiveBundle snapshot pin.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    BundleSuffix = ActiveBundle[bundle_suffix],
    TargetView = "v_" & BundleSuffix & "_earthquake_event_current",

    QuerySql = "SELECT " &
               "e.event_date_key_jst, e.event_date_jst, " &
               "CAST(SUBSTR(CAST(e.event_date_key_jst AS VARCHAR), 1, 6) AS INTEGER) AS year_month_jst, " &
               "e.region_key, e.region_name, e.region_category, " &
               "e.magnitude_band_code, e.depth_band_code, e.source_coverage_code, " &
               "COUNT(*) AS event_count, " &
               "COUNT(e.magnitude) AS magnitude_non_null_count, " &
               "SUM(e.magnitude) AS magnitude_sum, " &
               "AVG(e.magnitude) AS magnitude_avg, " &
               "MAX(e.magnitude) AS magnitude_max, " &
               "COUNT(e.depth_km) AS depth_non_null_count, " &
               "SUM(e.depth_km) AS depth_sum, " &
               "AVG(e.depth_km) AS depth_avg, " &
               "SUM(CASE WHEN e.magnitude >= 5.0 THEN 1 ELSE 0 END) AS strong_earthquake_count, " &
               "SUM(CASE WHEN e.tsunami_flag = true THEN 1 ELSE 0 END) AS tsunami_flagged_count, " &
               "SUM(CASE WHEN e.alert_level IS NOT NULL AND LOWER(TRIM(e.alert_level)) NOT IN ('', 'none') THEN 1 ELSE 0 END) AS alerted_event_count, " &
               "SUM(CASE WHEN e.max_intensity_code IS NOT NULL AND LOWER(TRIM(e.max_intensity_code)) NOT IN ('', 'none') THEN 1 ELSE 0 END) AS intensity_reported_count, " &
               "SUM(CASE WHEN e.region_category IN ('OFFSHORE', 'UNKNOWN') THEN 1 ELSE 0 END) AS offshore_or_unknown_count " &
               "FROM " & CatalogName & "." & SchemaName & "." & TargetView & " e " &
               "GROUP BY e.event_date_key_jst, e.event_date_jst, " &
               "CAST(SUBSTR(CAST(e.event_date_key_jst AS VARCHAR), 1, 6) AS INTEGER), " &
               "e.region_key, e.region_name, e.region_category, " &
               "e.magnitude_band_code, e.depth_band_code, e.source_coverage_code",

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
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
    #"Typed Table"
