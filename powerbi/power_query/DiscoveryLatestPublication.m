// ============================================================================
// Power Query M: DiscoveryLatestPublication
// Task: BI-01 (Power BI / Trino Connectivity)
// Purpose: Discovery query to inspect the latest verified publication in Trino.
//          Used BEFORE refresh to discover the desired publication_id and copy it
//          into Parameters[PinnedPublicationId] for fail-closed snapshot consistency.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],

    QuerySql = "SELECT publication_id, bundle_sha256, " &
               "SUBSTR(bundle_sha256, 1, 24) AS bundle_suffix, " &
               "serving_view, published_at_utc, max_event_time_utc, event_count " &
               "FROM " & CatalogName & "." & SchemaName & ".publication_status " &
               "WHERE verify_status = 'PASSED' " &
               "ORDER BY published_at_utc DESC LIMIT 1",

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
        {"publication_id", type text},
        {"bundle_sha256", type text},
        {"bundle_suffix", type text},
        {"serving_view", type text},
        {"published_at_utc", type datetimezone},
        {"max_event_time_utc", type datetimezone},
        {"event_count", Int64.Type}
    })
in
    #"Typed Table"
