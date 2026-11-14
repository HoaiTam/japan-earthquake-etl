// ============================================================================
// Power Query M Script: MetadataFreshness
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.publication_status (Verified published snapshot gate)
// Invariants:
// 1. Publication gate: Reads verified snapshot metadata with verify_status = 'PASSED'.
// 2. Data freshness: published_at_utc and max_event_time_utc strictly decoupled from refresh execution time.
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    StatusTable = CatalogName & "." & SchemaName & ".publication_status",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            publication_id,
            gold_run_id,
            snapshot_id,
            verify_status,
            published_at_utc,
            max_event_time_utc AS data_freshness_max_event_utc,
            event_count AS verified_event_count,
            CURRENT_TIMESTAMP AS report_refreshed_at_utc
        FROM " & StatusTable & "
        WHERE verify_status = 'PASSED'
        ORDER BY published_at_utc DESC
        LIMIT 1
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"publication_id", type text},
        {"gold_run_id", type text},
        {"snapshot_id", Int64.Type},
        {"verify_status", type text},
        {"published_at_utc", type datetimezone},
        {"data_freshness_max_event_utc", type datetimezone},
        {"verified_event_count", Int64.Type},
        {"report_refreshed_at_utc", type datetimezone}
    })
in
    TypedTable
