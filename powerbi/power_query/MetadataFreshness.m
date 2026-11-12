// ============================================================================
// Power Query M Script: MetadataFreshness
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: Aggregated metadata over iceberg.gold.event_current
// Invariants:
// 1. Freshness reporting: Latest event time in UTC and JST.
// 2. Exact row count parity verification.
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            MAX(event_time_utc) AS latest_event_time_utc,
            COUNT(*) AS total_canonical_events,
            COUNT(magnitude) AS total_measured_magnitudes,
            MIN(event_date_key_jst) AS min_date_key_jst,
            MAX(event_date_key_jst) AS max_date_key_jst,
            CURRENT_TIMESTAMP AS refreshed_at_utc
        FROM iceberg.gold.event_current
        WHERE is_earthquake = true
          AND is_in_study_area = true
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"latest_event_time_utc", type datetimezone},
        {"total_canonical_events", Int64.Type},
        {"total_measured_magnitudes", Int64.Type},
        {"min_date_key_jst", Int64.Type},
        {"max_date_key_jst", Int64.Type},
        {"refreshed_at_utc", type datetimezone}
    })
in
    TypedTable
