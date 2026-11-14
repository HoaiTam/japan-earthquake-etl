// ============================================================================
// Power Query Shared Query: ActiveBundle
// Task: BI-01 (Gold Serving & Pinned Publication Bundle)
// Purpose: Resolves and pins the exact verified publication bundle (publication_id + bundle_sha256)
//          shared across ALL fact, aggregate, dimension, and freshness queries during import/refresh.
// Guarantees:
// 1. Snapshot Consistency: Fact, dimensions, and aggregates read the exact same snapshot version.
// 2. Commit-before-publish immunity: Physical table commits not yet published are ignored.
// 3. Freshness Parity: Freshness metadata reflects the exact publication being queried.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    PinnedId = try Parameters[PinnedPublicationId] otherwise "",

    QuerySql = if PinnedId <> null and Text.Trim(PinnedId) <> "" then
        "SELECT publication_id, bundle_sha256, " &
        "SUBSTR(bundle_sha256, 1, 24) AS bundle_suffix, " &
        "serving_view, published_at_utc, max_event_time_utc, event_count " &
        "FROM " & CatalogName & "." & SchemaName & ".publication_status " &
        "WHERE publication_id = '" & Text.Replace(PinnedId, "'", "''") & "' " &
        "  AND verify_status = 'PASSED'"
    else
        "SELECT publication_id, bundle_sha256, " &
        "SUBSTR(bundle_sha256, 1, 24) AS bundle_suffix, " &
        "serving_view, published_at_utc, max_event_time_utc, event_count " &
        "FROM " & CatalogName & "." & SchemaName & ".publication_status " &
        "WHERE verify_status = 'PASSED' " &
        "ORDER BY published_at_utc DESC LIMIT 1",

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    ResolvedRecord = if Table.RowCount(Source) > 0 then Source{0} else error "No verified PASSED publication found in publication_status"
in
    ResolvedRecord
