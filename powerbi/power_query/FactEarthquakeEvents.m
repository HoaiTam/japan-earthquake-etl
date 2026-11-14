// ============================================================================
// Power Query M: FactEarthquakeEvents
// Task: BI-01 (Power BI / Trino Connectivity)
// Target: Verified, pinned canonical earthquake events for the active publication bundle.
// Source: Immutable versioned serving view v_<bundle_suffix>_earthquake_event_current.
// Guarantee: Exactly matches ActiveBundle snapshot pin; immune to unverified commits in physical tables.
// ============================================================================

let
    DsnName = Parameters[Dsn],
    CatalogName = Parameters[Catalog],
    SchemaName = Parameters[Schema],
    BundleSuffix = ActiveBundle[bundle_suffix],
    TargetView = "v_" & BundleSuffix & "_earthquake_event_current",

    QuerySql = "SELECT canonical_event_id, primary_observation_id, canonical_source_system, " &
               "event_time_utc, event_time_jst, event_date_utc, event_date_jst, " &
               "event_date_key_utc, event_date_key_jst, latitude, longitude, depth_km, " &
               "magnitude, magnitude_type, event_type_code, is_natural_earthquake, " &
               "place_name, is_in_study_area, region_key, region_name, region_category, " &
               "magnitude_band_code, depth_band_code, tsunami_flag, alert_level, " &
               "significance, max_intensity_code, determining_agency_code, catalog_era, " &
               "source_count, has_usgs, has_jma, source_coverage_code, link_status, " &
               "quality_status, canonical_model_version, gold_run_id, record_updated_at_utc " &
               "FROM " & CatalogName & "." & SchemaName & "." & TargetView,

    Source = Odbc.Query("dsn=" & DsnName, QuerySql),
    #"Added Strong Flag" = Table.AddColumn(Source, "is_strong_earthquake", each [magnitude] <> null and [magnitude] >= 5.0, type logical),
    #"Typed Table" = Table.TransformColumnTypes(#"Added Strong Flag", {
        {"canonical_event_id", type text},
        {"primary_observation_id", type text},
        {"canonical_source_system", type text},
        {"event_time_utc", type datetimezone},
        {"event_time_jst", type datetime},
        {"event_date_utc", type date},
        {"event_date_jst", type date},
        {"event_date_key_utc", Int64.Type},
        {"event_date_key_jst", Int64.Type},
        {"latitude", type number},
        {"longitude", type number},
        {"depth_km", type number},
        {"magnitude", type number},
        {"magnitude_type", type text},
        {"event_type_code", type text},
        {"is_natural_earthquake", type logical},
        {"place_name", type text},
        {"is_in_study_area", type logical},
        {"region_key", type text},
        {"region_name", type text},
        {"region_category", type text},
        {"magnitude_band_code", type text},
        {"depth_band_code", type text},
        {"tsunami_flag", type logical},
        {"alert_level", type text},
        {"significance", Int64.Type},
        {"max_intensity_code", type text},
        {"determining_agency_code", type text},
        {"catalog_era", type text},
        {"source_count", Int64.Type},
        {"has_usgs", type logical},
        {"has_jma", type logical},
        {"source_coverage_code", type text},
        {"link_status", type text},
        {"quality_status", type text},
        {"canonical_model_version", type text},
        {"gold_run_id", type text},
        {"record_updated_at_utc", type datetimezone},
        {"is_strong_earthquake", type logical}
    })
in
    #"Typed Table"
