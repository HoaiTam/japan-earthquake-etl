// ============================================================================
// Power Query M: DimMagnitudeBand
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Magnitude band dimension pinned to the active publication bundle.
// Source: Immutable versioned view v_<bundle_suffix>_dim_magnitude_band.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    BundleSuffix = ActiveBundle[bundle_suffix],
    TargetView = "v_" & BundleSuffix & "_dim_magnitude_band",

    QuerySql = "SELECT band_code, band_label, sort_order " &
               "FROM " & CatalogName & "." & SchemaName & "." & TargetView,

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Typed Table" = Table.TransformColumnTypes(Source, {
        {"band_code", type text},
        {"band_label", type text},
        {"sort_order", Int64.Type}
    })
in
    #"Typed Table"
