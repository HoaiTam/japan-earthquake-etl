// ============================================================================
// Power Query M Script: FactEarthquakeEvents
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.event_current (filtered to natural earthquakes in Japan study area)
// Invariants:
// 1. Zero double counting: groups canonical events only, never joins against bridge.
// 2. No direct Parquet reads: executed strictly via Trino ODBC gateway.
// 3. Strict explicit typing for VertiPaq engine optimization.
// ============================================================================

let
    // 1. Connect via Trino ODBC using environment parameter
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            event_id,
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
            is_strong_earthquake,
            tsunami_flag,
            alert_level,
            jma_max_intensity
        FROM iceberg.gold.event_current
        WHERE is_earthquake = true
          AND is_in_study_area = true
    "),

    // 2. Apply strict type transformations
    TypedTable = Table.TransformColumnTypes(Source, {
        {"event_id", type text},
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
        {"jma_max_intensity", type text}
    })
in
    TypedTable
