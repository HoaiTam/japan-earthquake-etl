// ============================================================================
// Power Query M Script: DimDepthBand
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_depth_band
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            band_code,
            band_label,
            min_depth_km,
            max_depth_km,
            sort_order
        FROM iceberg.gold.dim_depth_band
        ORDER BY sort_order
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"band_code", type text},
        {"band_label", type text},
        {"min_depth_km", type number},
        {"max_depth_km", type number},
        {"sort_order", Int64.Type}
    })
in
    TypedTable
