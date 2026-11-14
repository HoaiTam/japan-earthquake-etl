// ============================================================================
// Power Query M: DimRegion
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Geographic region dimension pinned to the active publication bundle.
// Source: Immutable versioned view v_<bundle_suffix>_dim_region.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    BundleSuffix = ActiveBundle[bundle_suffix],
    TargetView = "v_" & BundleSuffix & "_dim_region",

    QuerySql = "SELECT region_key, region_name, region_category " &
               "FROM " & CatalogName & "." & SchemaName & "." & TargetView,

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text}
    })
in
    #"Typed Table"
