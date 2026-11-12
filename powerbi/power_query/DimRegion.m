// ============================================================================
// Power Query M Script: DimRegion
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_region
// Invariant: Retains OFFSHORE and UNKNOWN categories; never drops offshore events.
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            region_key,
            region_name,
            region_category,
            latitude_min,
            latitude_max,
            longitude_min,
            longitude_max
        FROM iceberg.gold.dim_region
        ORDER BY region_name
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text},
        {"latitude_min", type number},
        {"latitude_max", type number},
        {"longitude_min", type number},
        {"longitude_max", type number}
    })
in
    TypedTable
