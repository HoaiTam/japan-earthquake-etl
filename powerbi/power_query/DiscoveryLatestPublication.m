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
    // Starburst ODBC formats Trino TIMESTAMP WITH TIME ZONE as "... UTC", which Table.TransformColumnTypes rejects as datetimezone.
    NormalizeUtc = (v as any) as nullable datetimezone =>
        if v = null then
            null
        else if Value.Is(v, type datetimezone) then
            v
        else if Value.Is(v, type datetime) then
            DateTime.AddZone(v, 0)
        else
            let
                raw = Text.Trim(Text.From(v)),
                baseText = if Text.EndsWith(raw, " UTC") then Text.Start(raw, Text.Length(raw) - 4) else raw,
                withOffset = if Text.EndsWith(raw, " UTC") then baseText & "+00:00" else raw
            in
                try DateTimeZone.FromText(withOffset, "en-US") otherwise DateTime.AddZone(DateTime.FromText(baseText, "en-US"), 0),
    #"Normalized UTC" = Table.TransformColumns(Source, {
        {"published_at_utc", NormalizeUtc, type datetimezone},
        {"max_event_time_utc", NormalizeUtc, type datetimezone}
    }),
    #"Typed Table" = Table.TransformColumnTypes(#"Normalized UTC", {
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
