package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;

/** Deterministic bounded metadata plan; actual upstream proof is resolved by ResearchCollectionJob. */
public final class ResearchCollectionPlan {
    public static final String VERSION = "dat03-mainshock-collection-v1";
    public static final int MAX_MAINSHOCKS = 4096;
    public record Limits(int mainshocksPerShard, long rowsPerShard, int maxShards) {
        public Limits {
            if (mainshocksPerShard < 1 || mainshocksPerShard > 128 || rowsPerShard < 1
                    || rowsPerShard > MAX_ROWS || maxShards < 1 || maxShards > 64)
                throw new IllegalArgumentException("COLLECTION_LIMITS_INVALID");
        }
        public static Limits defaults() { return new Limits(128, MAX_ROWS, 64); }
        public ObjectNode json() { return JSON.createObjectNode().put("mainshocks_per_shard",mainshocksPerShard)
                .put("rows_per_shard",rowsPerShard).put("max_shards",maxShards); }
        public static Limits read(JsonNode node) throws Exception {
            require(node.isObject() && node.size()==3 && node.path("mainshocks_per_shard").isIntegralNumber()
                    && node.path("mainshocks_per_shard").canConvertToInt() && node.path("rows_per_shard").isIntegralNumber()
                    && node.path("rows_per_shard").canConvertToLong() && node.path("max_shards").isIntegralNumber()
                    && node.path("max_shards").canConvertToInt(),"COLLECTION_TYPED_LIMITS_REQUIRED");
            return new Limits(node.path("mainshocks_per_shard").asInt(),node.path("rows_per_shard").asLong(),node.path("max_shards").asInt());
        }
    }
    public record Window(String id, long candidateRows) { }
    private ResearchCollectionPlan() { }

    public static ObjectNode create(JsonNode parent, JsonNode coverage, JsonNode mc, JsonNode exclusions,
            List<Window> windows, Limits limits) throws Exception {
        var publication=JSON.createObjectNode().put("verify_status","PASSED");
        for(String key:List.of("gold_table_name","physical_gold_table_name","gold_publication_id","publication_bundle_sha256","gold_snapshot_id")) publication.set(key,parent.path(key));
        var config=JSON.createObjectNode();
        for(String key:List.of("dataset_split","period_start_utc","period_end_utc_exclusive","observation_cutoff_utc")) config.set(key,parent.path(key));
        DatasetIdentityPlan.VERSIONS.forEach(key->config.set(key,parent.path(key)));
        config.set("filter_config",JSON.readTree(parent.path("filter_config_json").asText()));
        require(parent.path("gold_snapshot_id").isIntegralNumber() && parent.path("gold_snapshot_id").canConvertToLong()
                && parent.path("gold_snapshot_id").asLong()>0,"COLLECTION_PARENT_PIN_REQUIRED");
        // Jackson narrows small persisted longs to IntNode; normalize only the typed bigint pin.
        var normalized=(ObjectNode)parent.deepCopy(); normalized.put("gold_snapshot_id",parent.path("gold_snapshot_id").asLong());
        new DatasetIdentityPlan(publication,config,"collection-identity-check",java.time.Instant.EPOCH).assertRerun(normalized);
        require("BUILDING".equals(parent.path("dataset_status").asText()),"COLLECTION_BUILDING_PARENT_REQUIRED");
        String id=parent.path("dataset_id").asText();
        require(id.matches("ds_[a-f0-9]{64}") && parent.path("identity_payload_sha256").asText().equals(id.substring(3))
                && parent.path("gold_snapshot_id").isIntegralNumber() && parent.path("gold_snapshot_id").asLong()>0,
                "COLLECTION_PARENT_PIN_REQUIRED");
        require(WindowCoveragePolicy.STRICT.equals(WindowCoveragePolicy.fromManifest(parent)),"COLLECTION_STRICT_SPLIT_REQUIRED");
        int required="REPRODUCTION".equals(parent.path("dataset_split").asText())?225:
                "EXTENSION".equals(parent.path("dataset_split").asText())?63:0;
        require(required>0 && "COMPLETE".equals(coverage.path("status").asText())
                && coverage.path("required_month_count").isIntegralNumber() && coverage.path("required_month_count").asInt()==required
                && coverage.path("registry_sha256").asText().matches("[a-f0-9]{64}"),"COLLECTION_FULL_SPLIT_COVERAGE_REQUIRED");
        require("FULL_PINNED_SPLIT".equals(mc.path("scope").asText()) && mc.path("mc_value").isNumber()
                && Double.isFinite(mc.path("mc_value").asDouble()) && mc.path("mc_config_sha256").asText().matches("[a-f0-9]{64}"),
                "COLLECTION_GLOBAL_MC_REQUIRED");
        require(parent.path("dataset_id").equals(mc.path("dataset_id"))
                && parent.path("gold_snapshot_id").equals(mc.path("gold_snapshot_id"))
                && parent.path("period_start_utc").equals(mc.path("period_start_utc"))
                && parent.path("period_end_utc_exclusive").equals(mc.path("period_end_utc_exclusive"))
                && mc.path("mc_config_json").isTextual()
                && DatasetIdentityPlan.sha(mc.path("mc_config_json").asText()).equals(mc.path("mc_config_sha256").asText())
                && JSON.readTree(mc.path("mc_config_json").asText()).path("applied_mc").asDouble(Double.NaN)==mc.path("mc_value").asDouble(),
                "COLLECTION_MC_PIN_CONFLICT");
        WindowCoveragePolicy.validateReport(exclusions,windows.size());
        for(String field:List.of("dataset_split","period_start_utc","period_end_utc_exclusive"))
            require(parent.path(field).equals(exclusions.path(field)),"COLLECTION_PERIOD_CONFLICT");
        require(!windows.isEmpty() && windows.size()<=MAX_MAINSHOCKS,"COLLECTION_MAINSHOCK_METADATA_LIMIT");
        var excludedIds=new HashSet<String>(); exclusions.path("excluded_mainshocks").forEach(r->excludedIds.add(r.path("mainshock_event_id").asText()));
        var sorted=new TreeMap<String,Long>();
        for(var window:windows) require(window.id()!=null && !window.id().isBlank() && window.id().length()<=256
                && !excludedIds.contains(window.id()) && window.candidateRows()>0 && window.candidateRows()<=limits.rowsPerShard()
                && sorted.put(window.id(),window.candidateRows())==null,"COLLECTION_WINDOW_INVALID_OR_OVERSIZED");
        var identity=JSON.createObjectNode().put("collection_version",VERSION).put("dataset_id",id)
                .put("parent_identity_sha256",parent.path("identity_payload_sha256").asText());
        for(String field:List.of("gold_snapshot_id","gold_publication_id","physical_gold_table_name","dataset_split",
                "period_start_utc","period_end_utc_exclusive","observation_cutoff_utc","feature_version","scaling_config_version"))
            identity.set(field,parent.path(field));
        identity.set("coverage",coverage.deepCopy()); identity.set("global_mc",mc.deepCopy());
        identity.set("window_coverage",exclusions.deepCopy()); identity.set("layout_limits",limits.json());
        var shards=identity.putArray("shards"); ObjectNode shard=null; long total=0, rows=0; int count=0;
        for(var item:sorted.entrySet()) {
            if(shard==null || count==limits.mainshocksPerShard() || rows+item.getValue()>limits.rowsPerShard()) {
                require(shards.size()<limits.maxShards(),"COLLECTION_SHARD_LIMIT");
                shard=shards.addObject().put("shard_id",String.format(Locale.ROOT,"shard_%05d",shards.size()-1))
                        .put("mainshock_count",0).put("feature_row_count",0);
                shard.putArray("mainshocks"); rows=0; count=0;
            }
            shard.withArray("mainshocks").addObject().put("mainshock_event_id",item.getKey()).put("candidate_row_count",item.getValue());
            rows=Math.addExact(rows,item.getValue()); total=Math.addExact(total,item.getValue()); count++;
            shard.put("mainshock_count",count).put("feature_row_count",rows);
        }
        identity.put("mainshock_count",sorted.size()).put("feature_row_count",total)
                .put("full_mainshock_assignment_verified",true).put("split_windows",false);
        String hash=digest(identity);
        return identity.put("collection_id","col_"+hash).put("plan_sha256",hash);
    }
    public static void verify(JsonNode planned,JsonNode parent,JsonNode coverage,JsonNode mc,JsonNode exclusions,
            List<Window> windows,Limits limits) throws Exception {
        require(digest(planned).equals(digest(create(parent,coverage,mc,exclusions,windows,limits))),"COLLECTION_ASSIGNMENT_OR_PIN_CONFLICT");
    }
}
