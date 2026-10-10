package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import java.util.*;

/** Every data query uses a literal committed snapshot; no latest substitution. */
public final class GoldVerificationPlan {
    private final JsonNode commit;
    private final String namespace;
    public GoldVerificationPlan(JsonNode commit) {
        this.commit = commit.deepCopy(); namespace = commit.path("namespace").asText();
        if (!namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*")
                || !"COMMITTED".equals(commit.path("status").asText()) || commit.path("published").asBoolean(true)
                || !commit.path("identity_sha256").asText().matches("[a-f0-9]{64}")
                || !commit.path("gold_run_id").asText().matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))
            throw new IllegalArgumentException("EXACT_COMMIT_REQUIRED");
        var names = new HashSet<String>(); commit.path("tables").fieldNames().forEachRemaining(names::add);
        if (!names.equals(new HashSet<>(GoldIcebergWriter.TABLES))) throw new IllegalArgumentException("SIX_TABLE_BUNDLE_REQUIRED");
        for (String name : GoldIcebergWriter.TABLES) {
            var pin = commit.path("tables").path(name);
            if (!pin.path("snapshot_id").isIntegralNumber() || pin.path("snapshot_id").asLong() <= 0
                    || !pin.path("count").isIntegralNumber() || pin.path("count").asLong() < 0
                    || !table(name).equals(pin.path("table").asText())) throw new IllegalArgumentException("INVALID_SNAPSHOT_PIN");
        }
    }
    public String namespace() { return namespace; }
    public String table(String name) {
        if (!GoldIcebergWriter.TABLES.contains(name)) throw new IllegalArgumentException("TABLE_OUTSIDE_BUNDLE");
        return namespace+"."+name;
    }
    public long snapshot(String name) { return commit.path("tables").path(name).path("snapshot_id").asLong(); }
    public String ref(String name) { return table(name)+" FOR VERSION AS OF "+snapshot(name); }
    public long count(String name) { return commit.path("tables").path(name).path("count").asLong(); }
    public String metadata(String name, String suffix) { return namespace+".\""+name+"$"+suffix+"\""; }
    public Map<String,String> expectedTypes(String name) throws Exception {
        var schema = new ObjectMapper().readTree(commit.path("tables").path(name).path("schema_json").asText());
        var out = new TreeMap<String,String>();
        for (var field : schema.path("fields")) {
            String type = switch (field.path("type").asText()) {
                case "string" -> "varchar"; case "integer" -> "integer"; case "long" -> "bigint";
                case "boolean" -> "boolean"; case "double" -> "double"; case "date" -> "date";
                case "timestamp" -> "timestamp(6) with time zone"; case "timestamp_ntz" -> "timestamp(6)";
                default -> throw new IllegalArgumentException("UNSUPPORTED_GOLD_TYPE");
            };
            out.put(field.path("name").asText(), type);
        }
        if (out.isEmpty()) throw new IllegalArgumentException("EMPTY_GOLD_SCHEMA");
        return out;
    }
    public Map<String,String> blockers() {
        var queries = new LinkedHashMap<String,String>(); String e=ref("event_current"), b=ref("event_source_bridge");
        for (String name : GoldIcebergWriter.TABLES) {
            String key = name.equals("event_current") ? "canonical_event_id" : name.equals("event_source_bridge") ? "source_observation_id"
                : name.equals("dim_date") ? "date_key" : name.equals("dim_region") ? "region_key" : "band_code";
            queries.put("unique:"+name, "SELECT count(*)-count(DISTINCT "+key+") FROM "+ref(name));
        }
        String[] required={"schema_version","canonical_event_id","primary_observation_id","canonical_source_system","event_time_utc",
            "event_time_jst","event_date_utc","event_date_jst","event_date_key_utc","event_date_key_jst","latitude","longitude",
            "event_type_code","is_natural_earthquake","is_in_study_area","region_key","region_name","region_category",
            "magnitude_band_code","depth_band_code","source_count","has_usgs","has_jma","source_coverage_code","link_status",
            "quality_status","canonical_model_version","gold_run_id","record_updated_at_utc"};
        String nulls=String.join(" OR ",Arrays.stream(required).map(f->f+" IS NULL").toList());
        queries.put("required_fields", "SELECT count(*) FROM "+e+" WHERE "+nulls
            +" OR latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180 OR NOT is_finite(latitude) OR NOT is_finite(longitude)"
            +" OR (magnitude IS NOT NULL AND NOT is_finite(magnitude)) OR (depth_km IS NOT NULL AND NOT is_finite(depth_km))"
            +" OR schema_version <> '1.0' OR source_count NOT IN (1,2) OR quality_status NOT IN ('VALID','WARNING')"
            +" OR canonical_source_system NOT IN ('USGS','JMA_BULLETIN') OR region_category NOT IN ('PREFECTURE','OFFSHORE','UNKNOWN')"
            +" OR source_coverage_code <> CASE WHEN has_usgs AND has_jma THEN 'USGS_JMA' WHEN has_usgs THEN 'USGS_ONLY' ELSE 'JMA_ONLY' END"
            +" OR (has_jma AND (catalog_era IS NULL OR catalog_era NOT IN ('LEGACY','UNIFIED')))"
            +" OR (NOT has_jma AND catalog_era IS NOT NULL)"
            +" OR is_natural_earthquake <> (event_type_code = 'EARTHQUAKE')");
        queries.put("bridge_lineage", "SELECT count(*) FROM "+b+" b LEFT JOIN "+e+" e ON b.canonical_event_id=e.canonical_event_id"
            +" WHERE e.canonical_event_id IS NULL OR b.source_system NOT IN ('USGS','JMA_BULLETIN') OR b.source_system IS NULL"
            +" OR b.source_record_key IS NULL OR b.raw_object_uri IS NULL OR NOT starts_with(b.raw_object_uri,'s3://')"
            +" OR b.bronze_manifest_id IS NULL OR b.is_primary IS NULL"
            +" OR (b.source_system='JMA_BULLETIN' AND b.catalog_release IS NULL)"
            +" OR b.event_month_utc <> format_datetime(e.event_time_utc,'yyyy-MM')");
        queries.put("source_counts", "SELECT count(*) FROM "+e+" e LEFT JOIN (SELECT canonical_event_id,count(*) n,count_if(is_primary) p,"
            +"count_if(source_system='USGS') u,count_if(source_system='JMA_BULLETIN') j FROM "+b+" GROUP BY canonical_event_id) b"
            +" ON e.canonical_event_id=b.canonical_event_id WHERE b.n IS NULL OR b.n<>e.source_count OR b.p<>1 OR b.u>1 OR b.j>1"
            +" OR e.has_usgs<>(b.u>0) OR e.has_jma<>(b.j>0)");
        queries.put("primary_provenance", "SELECT count(*) FROM "+e+" e LEFT JOIN "+b+" b ON e.canonical_event_id=b.canonical_event_id AND b.is_primary"
            +" WHERE b.source_observation_id IS NULL OR b.source_observation_id<>e.primary_observation_id OR b.source_system<>e.canonical_source_system");
        queries.put("utc_jst", "SELECT count(*) FROM "+e+" WHERE event_time_jst<>CAST(event_time_utc AT TIME ZONE 'Asia/Tokyo' AS timestamp(6))"
            +" OR event_date_utc<>CAST(event_time_utc AT TIME ZONE 'UTC' AS date) OR event_date_jst<>CAST(event_time_jst AS date)"
            +" OR event_date_key_utc<>CAST(format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyyMMdd') AS integer)"
            +" OR event_date_key_jst<>CAST(format_datetime(event_time_jst,'yyyyMMdd') AS integer)"
            +" OR (canonical_source_system='JMA_BULLETIN' AND catalog_era<>CASE WHEN event_time_jst<TIMESTAMP '1997-10-01 00:00:00' THEN 'LEGACY' ELSE 'UNIFIED' END)");
        queries.put("dimensions", "SELECT count(*) FROM "+e+" e LEFT JOIN "+ref("dim_date")+" d ON e.event_date_key_utc=d.date_key"
            +" LEFT JOIN "+ref("dim_date")+" j ON e.event_date_key_jst=j.date_key LEFT JOIN "+ref("dim_region")+" r ON e.region_key=r.region_key"
            +" LEFT JOIN "+ref("dim_magnitude_band")+" m ON e.magnitude_band_code=m.band_code LEFT JOIN "+ref("dim_depth_band")+" b ON e.depth_band_code=b.band_code"
            +" WHERE d.date_key IS NULL OR j.date_key IS NULL OR r.region_key IS NULL OR m.band_code IS NULL OR b.band_code IS NULL"
            +" OR d.calendar_date<>e.event_date_utc OR j.calendar_date<>e.event_date_jst OR r.region_name<>e.region_name OR r.region_category<>e.region_category");
        queries.put("aggregate_count", "SELECT abs((SELECT coalesce(sum(source_count),0) FROM "+e+")-(SELECT count(*) FROM "+b+"))");
        var affected=new ArrayList<String>();commit.path("scope").path("affected_months").forEach(m->{
            java.time.YearMonth.parse(m.asText());affected.add(quote(m.asText()));});
        if(affected.isEmpty())throw new IllegalArgumentException("AFFECTED_MONTHS_REQUIRED");
        queries.put("changed_run_scope","SELECT count(*) FROM "+e+" WHERE gold_run_id="+quote(commit.path("gold_run_id").asText())
            +" AND format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyy-MM') NOT IN ("+String.join(",",affected)+")");
        return Collections.unmodifiableMap(queries);
    }
    public String outsideScope(String name) {
        long baseline=commit.path("scope").path("baseline_snapshots").path(name).asLong();
        if(baseline==0) return "SELECT 0";
        var months=new ArrayList<String>(); commit.path("scope").path("affected_months").forEach(m->{
            java.time.YearMonth.parse(m.asText()); months.add(quote(m.asText())); });
        String field=name.equals("event_current") ? "format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyy-MM')" : "event_month_utc";
        String filter=" WHERE "+field+" NOT IN ("+String.join(",",months)+")";
        String before="SELECT * FROM "+table(name)+" FOR VERSION AS OF "+baseline+filter, after="SELECT * FROM "+ref(name)+filter;
        return "SELECT count(*) FROM (("+before+" EXCEPT "+after+") UNION ALL ("+after+" EXCEPT "+before+")) changes";
    }
    public static String quote(String value) { return "'"+value.replace("'","''")+"'"; }
}
