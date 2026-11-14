// ============================================================================
// Power Query M: MetadataFreshness
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Verified publication status and data freshness pinned to ActiveBundle.
// Source: iceberg.gold.publication_status filtered by ActiveBundle[publication_id].
// Guarantee: Freshness strictly reflects the publication bundle currently queried,
//            preventing split-brain when publication rows are committed before alias switch.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    PubId = ActiveBundle[publication_id],

    QuerySql = "SELECT publication_id, bundle_sha256, published_at_utc, " &
               "max_event_time_utc AS data_freshness_max_event_utc, " &
               "event_count AS published_event_count, " &
               "CURRENT_TIMESTAMP AS report_refreshed_at_utc " &
               "FROM " & CatalogName & "." & SchemaName & ".publication_status " &
               "WHERE publication_id = '" & Text.Replace(PubId, "'", "''") & "' " &
               "  AND verify_status = 'PASSED'",

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
        {"publication_id", type text},
        {"bundle_sha256", type text},
        {"published_at_utc", type datetimezone},
        {"data_freshness_max_event_utc", type datetimezone},
        {"published_event_count", Int64.Type},
        {"report_refreshed_at_utc", type datetimezone}
    })
in
    #"Typed Table"
