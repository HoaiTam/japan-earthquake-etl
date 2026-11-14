// ============================================================================
// Power Query Shared Query: ActiveBundle
// Task: BI-01 (Gold Serving & Pinned Publication Bundle)
// Purpose: Resolves and validates the explicitly pinned verified publication bundle
//          (publication_id + bundle_sha256) for ALL fact, aggregate, dimension,
//          and freshness queries during import/refresh.
// Guarantees:
// 1. Fail-closed Snapshot Consistency: Bắt buộc PinnedPublicationId không rỗng.
//    Loại bỏ hoàn toàn auto-resolve LIMIT 1 để ngăn chặn race condition / split-brain
//    khi Power Query đánh giá các query độc lập trên nhiều thread.
// 2. Commit-before-publish immunity: Chỉ đọc bundle có verify_status = 'PASSED'.
// 3. Freshness Parity: Freshness metadata phản ánh đúng publication_id đã ghim.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    RawPinnedId = try Parameters[PinnedPublicationId] otherwise "",

    // Fail-closed validation: PinnedPublicationId is strictly mandatory
    PinnedId = if RawPinnedId = null or Text.Trim(RawPinnedId) = "" then
        error "PinnedPublicationId parameter is mandatory for snapshot consistency. Please set a valid publication_id in Parameters.m or run DiscoveryLatestPublication to find one."
    else
        Text.Trim(RawPinnedId),

    QuerySql = "SELECT publication_id, bundle_sha256, " &
               "SUBSTR(bundle_sha256, 1, 24) AS bundle_suffix, " &
               "serving_view, published_at_utc, max_event_time_utc, event_count " &
               "FROM " & CatalogName & "." & SchemaName & ".publication_status " &
               "WHERE publication_id = '" & Text.Replace(PinnedId, "'", "''") & "' " &
               "  AND verify_status = 'PASSED'",

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    ResolvedRecord = if Table.RowCount(Source) > 0 then
        Source{0}
    else
        error ("Publication '" & PinnedId & "' not found with verify_status = 'PASSED' in publication_status")
in
    ResolvedRecord
