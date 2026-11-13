// ============================================================================
// Power Query Configuration: Trino Connection Parameters
// Task: BI-01 (Power BI / Trino Connectivity)
// Note: Centralized configuration record for Trino ODBC connection.
//       Referenced by all serving queries as Parameters[Dsn], Parameters[Catalog], etc.
//       Contains ZERO hardcoded credentials.
// ============================================================================

let
    TrinoConfig = [
        // System DSN configured in Windows 64-bit ODBC Administrator
        Dsn = "TrinoJapanEarthquake",
        // Target Iceberg catalog name in Trino
        Catalog = "iceberg",
        // Target schema for Gold serving views and publication tables
        Schema = "gold"
    ]
in
    TrinoConfig
