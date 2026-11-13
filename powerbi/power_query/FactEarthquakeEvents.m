// ============================================================================
// Power Query M Script: FactEarthquakeEvents
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.earthquake_event_current (Verified published natural earthquakes in Japan study area)
// Invariants:
// 1. Zero double counting: groups canonical events only, never joins against bridge.
// 2. Verified published scope: queries the published earthquake_event_current view.
// 3. Strict explicit typing for VertiPaq engine optimization.
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    TargetTable = CatalogName & "." & SchemaName & ".earthquake_event_current",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            canonical_event_id,
            event_date_key_jst,
            event_date_jst,
            event_time_utc,
            latitude,
            longitude,
            depth_km,
            magnitude,
            region_key,
            region_name,
            region_category,
            magnitude_band_code,
            depth_band_code,
            source_coverage_code,
            CASE WHEN magnitude >= 5.0 THEN true ELSE false END AS is_strong_earthquake,
            tsunami_flag,
            alert_level,
            max_intensity_code
        FROM " & TargetTable & "
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"canonical_event_id", type text},
        {"event_date_key_jst", Int64.Type},
        {"event_date_jst", type date},
        {"event_time_utc", type datetimezone},
        {"latitude", type number},
        {"longitude", type number},
        {"depth_km", type number},
        {"magnitude", type number},
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text},
        {"magnitude_band_code", type text},
        {"depth_band_code", type text},
        {"source_coverage_code", type text},
        {"is_strong_earthquake", type logical},
        {"tsunami_flag", type logical},
        {"alert_level", type text},
        {"max_intensity_code", type text}
    })
in
    TypedTable
