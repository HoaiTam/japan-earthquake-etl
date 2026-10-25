package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class ResearchCollectionPlanTest {
    static ObjectNode parent() {
        var c=DatasetManifestTest.config("REPRODUCTION");
        ((ObjectNode)c.path("filter_config")).put("window_coverage_policy",WindowCoveragePolicy.STRICT);
        return (ObjectNode)new DatasetIdentityPlan(DatasetManifestTest.publication(),c,"collection-test",Instant.EPOCH).manifest();
    }
    static ObjectNode coverage() {return JSON.createObjectNode().put("status","COMPLETE").put("required_month_count",225).put("registry_sha256","c".repeat(64));}
    static ObjectNode mc(ObjectNode parent) {
        String config="{\"applied_mc\":1.0}";
        var m=JSON.createObjectNode().put("scope","FULL_PINNED_SPLIT").put("mc_value",1.)
                .put("mc_config_json",config).put("mc_config_sha256",DatasetIdentityPlan.sha(config));
        for(String field:List.of("dataset_id","gold_snapshot_id","period_start_utc","period_end_utc_exclusive")) m.set(field,parent.path(field));
        return m;
    }
    static ObjectNode exclusions(long count) {
        var p=parent(); var r=JSON.createObjectNode().put("policy_version",WindowCoveragePolicy.STRICT)
                .put("eligible_mainshock_count",count).put("included_mainshock_count",count).put("excluded_mainshock_count",0)
                .put("windows_clipped",false).put("cross_split_data_used",false);
        for(String field:List.of("dataset_split","period_start_utc","period_end_utc_exclusive")) r.set(field,p.path(field));
        r.putArray("excluded_mainshocks"); r.putObject("reason_counts"); return r;
    }
    static List<ResearchCollectionPlan.Window> windows(int count,long rows) {
        var result=new ArrayList<ResearchCollectionPlan.Window>();
        for(int i=0;i<count;i++) result.add(new ResearchCollectionPlan.Window("main-"+String.format(Locale.ROOT,"%04d",i),rows));
        return result;
    }
    static ObjectNode plan(List<ResearchCollectionPlan.Window> windows,ResearchCollectionPlan.Limits limits) throws Exception {
        var p=parent(); return ResearchCollectionPlan.create(p,coverage(),mc(p),exclusions(windows.size()),windows,limits);
    }
    @Test void moreThan128MainshocksHaveFullDeterministicAssignmentWithoutWindowSplit() throws Exception {
        var list=windows(249,100); var p=plan(list,ResearchCollectionPlan.Limits.defaults());
        assertEquals(2,p.path("shards").size()); assertEquals(249,p.path("mainshock_count").asInt());
        assertEquals(24900,p.path("feature_row_count").asInt()); assertEquals(128,p.path("shards").get(0).path("mainshock_count").asInt());
        Collections.reverse(list); assertEquals(p,plan(list,ResearchCollectionPlan.Limits.defaults()));
        assertFalse(p.path("split_windows").asBoolean(true));
    }
    @Test void rowBudgetPacksWholeWindowsAndNeverTruncates() throws Exception {
        var p=plan(windows(3,60000),ResearchCollectionPlan.Limits.defaults());
        assertEquals(3,p.path("shards").size()); assertEquals(180000,p.path("feature_row_count").asInt());
        assertThrows(Exception.class,()->plan(windows(1,100001),ResearchCollectionPlan.Limits.defaults()));
        assertThrows(Exception.class,()->plan(windows(3,1),new ResearchCollectionPlan.Limits(1,10,2)));
    }
    @Test void everyAssignmentIsCheckedAgainstExactGlobalInventory() throws Exception {
        var list=windows(3,1); var limits=ResearchCollectionPlan.Limits.defaults(); var p=plan(list,limits); var parent=parent();
        ResearchCollectionPlan.verify(p,parent,coverage(),mc(parent),exclusions(3),list,limits);
        var removed=windows(2,1);
        assertThrows(Exception.class,()->ResearchCollectionPlan.verify(p,parent,coverage(),mc(parent),exclusions(2),removed,limits));
        var forged=p.deepCopy(); ((com.fasterxml.jackson.databind.node.ArrayNode)forged.path("shards").get(0).path("mainshocks")).remove(0);
        assertThrows(Exception.class,()->ResearchCollectionPlan.verify(forged,parent,coverage(),mc(parent),exclusions(3),list,limits));
    }
    @Test void duplicateEmptyOversizedMetadataAndInvalidLimitsFailClosed() {
        var duplicate=windows(2,1); duplicate.add(duplicate.get(0));
        assertThrows(Exception.class,()->plan(duplicate,ResearchCollectionPlan.Limits.defaults()));
        assertThrows(Exception.class,()->plan(List.of(),ResearchCollectionPlan.Limits.defaults()));
        assertThrows(Exception.class,()->plan(windows(4097,1),ResearchCollectionPlan.Limits.defaults()));
        assertThrows(Exception.class,()->plan(windows(1,0),ResearchCollectionPlan.Limits.defaults()));
        assertThrows(IllegalArgumentException.class,()->new ResearchCollectionPlan.Limits(129,100000,64));
        assertThrows(IllegalArgumentException.class,()->new ResearchCollectionPlan.Limits(128,100001,64));
        assertThrows(Exception.class,()->ResearchCollectionPlan.Limits.read(JSON.createObjectNode().put("mainshocks_per_shard",true).put("rows_per_shard",100).put("max_shards",2)));
    }
    @Test void layoutAndGoldMcCoveragePinsAreIdentityNotRuntimeClock() throws Exception {
        var list=windows(3,1); var original=plan(list,ResearchCollectionPlan.Limits.defaults());
        assertNotEquals(original.path("collection_id"),plan(list,new ResearchCollectionPlan.Limits(1,100000,64)).path("collection_id"));
        var parent=parent(); var c=coverage(); c.put("registry_sha256","d".repeat(64));
        assertNotEquals(original.path("collection_id"),ResearchCollectionPlan.create(parent,c,mc(parent),exclusions(3),list,ResearchCollectionPlan.Limits.defaults()).path("collection_id"));
        parent.put("created_at_utc","2026-10-24T00:00:00Z");
        assertEquals(original,ResearchCollectionPlan.create(parent,coverage(),mc(parent),exclusions(3),list,ResearchCollectionPlan.Limits.defaults()));
        parent.put("gold_snapshot_id",100);
        assertThrows(Exception.class,()->ResearchCollectionPlan.create(parent,coverage(),mc(parent),exclusions(3),list,ResearchCollectionPlan.Limits.defaults()));
    }
    @Test void callerMcOrPartialCoverageCannotAuthorizeCollection() throws Exception {
        var parent=parent(); var list=windows(1,1); var config=mc(parent);
        config.put("scope","SHARD"); assertThrows(Exception.class,()->ResearchCollectionPlan.create(parent,coverage(),config,exclusions(1),list,ResearchCollectionPlan.Limits.defaults()));
        config.setAll(mc(parent)); config.put("mc_value",9);
        assertThrows(Exception.class,()->ResearchCollectionPlan.create(parent,coverage(),config,exclusions(1),list,ResearchCollectionPlan.Limits.defaults()));
        var c=coverage(); c.put("required_month_count",1);
        assertThrows(Exception.class,()->ResearchCollectionPlan.create(parent,c,mc(parent),exclusions(1),list,ResearchCollectionPlan.Limits.defaults()));
    }
    @Test void persistedJsonBigintNodeWidthDoesNotChangeIdentity() throws Exception {
        var parent=(ObjectNode)JSON.readTree(parent().toString());
        var p=ResearchCollectionPlan.create(parent,coverage(),mc(parent),exclusions(1),windows(1,1),ResearchCollectionPlan.Limits.defaults());
        assertEquals(plan(windows(1,1),ResearchCollectionPlan.Limits.defaults()).path("collection_id"),p.path("collection_id"));
        parent.put("gold_snapshot_id",99.0);
        assertThrows(Exception.class,()->ResearchCollectionPlan.create(parent,coverage(),mc(parent),exclusions(1),windows(1,1),ResearchCollectionPlan.Limits.defaults()));
    }
}
