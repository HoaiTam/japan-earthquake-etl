// ============================================================================
// Power Query M Script: DimDate
// Task: BI-01 (Power BI / Trino Connectivity)
// Source: iceberg.gold.dim_date (Matches GoldEventTransformer output schema)
// ============================================================================

let
    Config = try Parameters otherwise [Dsn = "TrinoJapanEarthquake", Catalog = "iceberg", Schema = "gold"],
    DsnName = Config[Dsn],
    CatalogName = Config[Catalog],
    SchemaName = Config[Schema],
    TargetTable = CatalogName & "." & SchemaName & ".dim_date",

    Source = Odbc.Query("dsn=" & DsnName, "
        SELECT
            date_key,
            calendar_date,
            year,
            quarter,
            month,
            month_name,
            day_of_month,
            day_of_week,
            is_weekend
        FROM " & TargetTable & "
        ORDER BY date_key
    "),

    TypedTable = Table.TransformColumnTypes(Source, {
        {"date_key", Int64.Type},
        {"calendar_date", type date},
        {"year", Int64.Type},
        {"quarter", Int64.Type},
        {"month", Int64.Type},
        {"month_name", type text},
        {"day_of_month", Int64.Type},
        {"day_of_week", Int64.Type},
        {"is_weekend", type logical}
    })
in
    TypedTable
