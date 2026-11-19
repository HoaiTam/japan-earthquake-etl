// ============================================================================
// Power Query M: DimDate
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Date dimension pinned to the active publication bundle.
// Source: Immutable versioned view v_<bundle_suffix>_dim_date.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    BundleSuffix = ActiveBundle[bundle_suffix],
    TargetView = "v_" & BundleSuffix & "_dim_date",

    QuerySql = "SELECT date_key, calendar_date, year, quarter, month, month_name, " &
               "day_of_month, day_of_week, is_weekend " &
               "FROM " & CatalogName & "." & SchemaName & "." & TargetView,

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
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
    #"Typed Table"
