// ============================================================================
// Power Query M Script: DimDate
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_date
// ============================================================================

let
    DsnName = try pTrinoDsn otherwise "TrinoJapanEarthquake",
    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            date_key,
            full_date,
            year,
            month,
            month_name,
            day,
            day_of_week,
            day_name,
            is_weekend
        FROM iceberg.gold.dim_date
        ORDER BY date_key
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"date_key", Int64.Type},
        {"full_date", type date},
        {"year", Int64.Type},
        {"month", Int64.Type},
        {"month_name", type text},
        {"day", Int64.Type},
        {"day_of_week", Int64.Type},
        {"day_name", type text},
        {"is_weekend", type logical}
    })
in
    TypedTable
