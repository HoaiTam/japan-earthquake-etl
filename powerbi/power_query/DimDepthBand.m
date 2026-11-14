// ============================================================================
// Power Query M Script: DimDepthBand
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_depth_band (Matches GoldEventTransformer output schema)
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    TargetTable = CatalogName & "." & SchemaName & ".dim_depth_band",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            band_code,
            band_label,
            sort_order
        FROM " & TargetTable & "
        ORDER BY sort_order
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"band_code", type text},
        {"band_label", type text},
        {"sort_order", Int64.Type}
    })
in
    TypedTable
