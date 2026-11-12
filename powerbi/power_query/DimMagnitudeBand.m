// ============================================================================
// Power Query M Script: DimMagnitudeBand
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_magnitude_band
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            band_code,
            band_label,
            min_magnitude,
            max_magnitude,
            sort_order
        FROM iceberg.gold.dim_magnitude_band
        ORDER BY sort_order
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"band_code", type text},
        {"band_label", type text},
        {"min_magnitude", type number},
        {"max_magnitude", type number},
        {"sort_order", Int64.Type}
    })
in
    TypedTable
