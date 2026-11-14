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
        // Host endpoint (127.0.0.1 on Windows host machine)
        Host = "127.0.0.1",
        // Host port mapped from TRINO_HOST_PORT in .env (default 8081 for host, container port 8080)
        HostPort = 8081,
        // Target Iceberg catalog name in Trino
        Catalog = "iceberg",
        // Target schema for Gold serving views and publication tables
        Schema = "gold",
        // Optional pinned publication ID (e.g. "pub_..." to pin a specific verified bundle).
        // Leave empty string "" to auto-resolve the active verified PASSED publication bundle.
        PinnedPublicationId = ""
    ]
in
    TrinoConfig
