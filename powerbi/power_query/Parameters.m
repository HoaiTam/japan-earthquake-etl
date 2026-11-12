// ============================================================================
// Power Query Parameters: Trino Connection Configuration
// Task: BI-01 (Power BI / Trino Connectivity)
// Note: These parameters allow switching environments (local Docker, staging, prod)
//       without modifying individual query code and WITHOUT storing credentials.
// ============================================================================

let
    // Default DSN configured in Windows 64-bit ODBC Data Source Administrator
    pTrinoDsn = "TrinoJapanEarthquake" meta [
        IsParameterQuery = true,
        Type = type text,
        Description = "ODBC DSN Name configured in Windows 64-bit ODBC Administrator"
    ],

    // Local loopback host for Trino coordinator container
    pTrinoHost = "127.0.0.1" meta [
        IsParameterQuery = true,
        Type = type text,
        Description = "Trino coordinator host or IP"
    ],

    // Trino host port exposed by Docker Compose (default 8080 or 8081)
    pTrinoPort = 8080 meta [
        IsParameterQuery = true,
        Type = type number,
        Description = "Trino coordinator HTTP port"
    ],

    // Iceberg REST catalog name registered in Trino
    pTrinoCatalog = "iceberg" meta [
        IsParameterQuery = true,
        Type = type text,
        Description = "Trino catalog name for Iceberg tables"
    ],

    // Target schema for Gold serving tables and views
    pTrinoSchema = "gold" meta [
        IsParameterQuery = true,
        Type = type text,
        Description = "Trino schema name for Gold layer"
    ]
in
    pTrinoDsn
