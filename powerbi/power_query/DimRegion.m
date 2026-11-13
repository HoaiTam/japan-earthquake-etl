// ============================================================================
// Power Query M Script: DimRegion
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_region (Matches GoldEventTransformer output schema)
// Invariant: Retains OFFSHORE and UNKNOWN categories; never drops offshore events.
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    TargetTable = CatalogName & "." & SchemaName & ".dim_region",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            region_key,
            region_name,
            region_category
        FROM " & TargetTable & "
        ORDER BY region_name
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text}
    })
in
    TypedTable
