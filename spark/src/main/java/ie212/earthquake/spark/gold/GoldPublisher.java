package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import static ie212.earthquake.spark.gold.GoldVerificationPlan.quote;

/** Trino is the independent verification engine; only this gate creates Published metadata. */
public final class GoldPublisher {
    public record Result(List<JsonNode> columns, List<JsonNode> rows) { }
    @FunctionalInterface public interface Sql { Result execute(String sql) throws Exception; }
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Sql sql;
    private final SilverObjectStore store;
    private final Runnable lease;
    private final boolean operational;
    public static final String BUCKETED_OUTSIDE_VERSION="dat03-exact-outside-buckets-v1";
    private final boolean bucketedOutside;
    public GoldPublisher(Sql sql, SilverObjectStore store, Runnable lease) {
        this(sql,store,lease,false);
    }
    public GoldPublisher(Sql sql, SilverObjectStore store, Runnable lease, boolean operational) {
        this(sql,store,lease,operational,false);
    }
    private GoldPublisher(Sql sql,SilverObjectStore store,Runnable lease,boolean operational,boolean bucketedOutside) {
        this.sql=Objects.requireNonNull(sql);this.store=Objects.requireNonNull(store);this.lease=Objects.requireNonNull(lease);
        this.operational=operational;this.bucketedOutside=bucketedOutside;
    }
    /** Opt-in only for isolated DAT-03 catalog namespaces; legacy/operational behavior stays unchanged. */
    public static GoldPublisher researchRollout(Sql sql,SilverObjectStore store,Runnable lease) {
        return new GoldPublisher(sql,store,lease,false,true);
    }
    /** Read-only ORC-07 gate: no views, PASSED rows, aliases or publication journals. */
    public JsonNode verify(String goldRun,String namespace,String identity) throws Exception {
        lease.run();
        if(goldRun==null || !goldRun.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) throw new IOException("EXACT_GOLD_RUN_REQUIRED");
        byte[] bytes=store.read("gold-commits/"+goldRun+"/commit.json");
        var commit=JSON.readTree(bytes);
        policy(commit);
        if(!goldRun.equals(commit.path("gold_run_id").asText()) || !namespace.equals(commit.path("namespace").asText())
                || !identity.equals(commit.path("identity_sha256").asText())) throw new IOException("COMMIT_PIN_MISMATCH");
        var plan=new GoldVerificationPlan(commit); var report=JSON.createObjectNode();
        report.put("engine","TRINO"); report.put("gold_run_id",goldRun); report.put("namespace",namespace);
        report.put("identity_sha256",identity); report.put("bundle_sha256",SilverBundlePublisher.sha(bytes));
        if(operational) OperationalGoldPolicy.attach(report,commit.path("operational_policy"));
        if(bucketedOutside)report.put("outside_scope_strategy",BUCKETED_OUTSIDE_VERSION);
        report.set("snapshot_bundle",commit.path("tables")); var checks=report.putObject("checks");
        for(String name:GoldIcebergWriter.TABLES) {
            var shape=sql.execute("SELECT * FROM "+plan.ref(name)+" LIMIT 0");
            Map<String,String> actual=new TreeMap<>(); shape.columns().forEach(c->actual.put(c.path("name").asText(),c.path("type").asText()));
            if(!actual.equals(plan.expectedTypes(name))) throw new IOException("TRINO_SCHEMA_MISMATCH:"+name);
            checked(checks,"count:"+name,"SELECT count(*) FROM "+plan.ref(name),plan.count(name));
            checked(checks,"identity:"+name,"SELECT count(*) FROM "+plan.metadata(name,"snapshots")+" WHERE snapshot_id="+plan.snapshot(name)
                    +" AND summary['gold.operation_id']="+quote(goldRun)+" AND summary['gold.identity_sha256']="+quote(identity)
                    +(operational ? "" : OperationalGoldPolicy.researchSnapshotPredicate()),1);
        }
        for(var query:plan.blockers().entrySet()) checked(checks,query.getKey(),query.getValue(),0);
        operationalChecks(commit,plan,checks);
        outside(plan,checks);
        report.put("verify_status","PASSED"); report.put("published",false); return report;
    }
    public JsonNode publish(String goldRun, String expectedNamespace, String expectedIdentity) throws Exception {
        lease.run();
        if(goldRun==null || !goldRun.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) throw new IllegalArgumentException("EXACT_GOLD_RUN_REQUIRED");
        byte[] commitBytes=store.read("gold-commits/"+goldRun+"/commit.json");
        JsonNode commit=JSON.readTree(commitBytes);
        policy(commit);
        if(!goldRun.equals(commit.path("gold_run_id").asText()) || !Objects.equals(expectedNamespace,commit.path("namespace").asText())
                || !Objects.equals(expectedIdentity,commit.path("identity_sha256").asText())) throw new IOException("COMMIT_PIN_MISMATCH");
        var plan=new GoldVerificationPlan(commit); String namespace=plan.namespace();
        String bundleSha=SilverBundlePublisher.sha(commitBytes), publicationId="pub_"+bundleSha;
        String journal="gold-verification/"+goldRun+"/publication.json";
        ObjectNode report=JSON.createObjectNode();report.put("task","GLD-04");report.put("engine","TRINO");
        report.put("publication_id",publicationId); report.put("bundle_sha256",bundleSha);report.put("identity_sha256",expectedIdentity);
        report.put("gold_run_id",goldRun);report.put("namespace",namespace);report.set("snapshot_bundle",commit.path("tables"));
        if(operational) OperationalGoldPolicy.attach(report,commit.path("operational_policy"));
        if(bucketedOutside)report.put("outside_scope_strategy",BUCKETED_OUTSIDE_VERSION);
        var checks=report.putObject("checks");
        // First verify all exact snapshots. No view/metadata mutation precedes these blockers.
        for(String name:GoldIcebergWriter.TABLES) {
            var shape=sql.execute("SELECT * FROM "+plan.ref(name)+" LIMIT 0");
            Map<String,String> actual=new TreeMap<>();shape.columns().forEach(c->actual.put(c.path("name").asText(),c.path("type").asText()));
            if(!actual.equals(plan.expectedTypes(name))) throw new IOException("TRINO_SCHEMA_MISMATCH:"+name);
            checked(checks,"count:"+name,"SELECT count(*) FROM "+plan.ref(name),plan.count(name));
            checked(checks,"identity:"+name,"SELECT count(*) FROM "+plan.metadata(name,"snapshots")+" WHERE snapshot_id="+plan.snapshot(name)
                +" AND summary['gold.operation_id']="+quote(goldRun)+" AND summary['gold.identity_sha256']="+quote(expectedIdentity)
                +(operational ? "" : OperationalGoldPolicy.researchSnapshotPredicate()),1);
        }
        for(var query:plan.blockers().entrySet()) checked(checks,query.getKey(),query.getValue(),0);
        operationalChecks(commit,plan,checks);
        outside(plan,checks);
        if(store.exists(journal)) {
            JsonNode existing=JSON.readTree(store.read(journal));
            if(!publicationId.equals(existing.path("publication_id").asText()) || !"PASSED".equals(existing.path("verify_status").asText())
                    || !existing.path("published").asBoolean(false)) throw new IOException("PUBLICATION_JOURNAL_CONFLICT");
            if(operational && !existing.path("operational_policy").equals(commit.path("operational_policy"))) throw new IOException("ASOF_PUBLICATION_POLICY_CHANGED");
            if(bucketedOutside != existing.has("outside_scope_strategy") || (bucketedOutside
                    && !BUCKETED_OUTSIDE_VERSION.equals(existing.path("outside_scope_strategy").asText())))
                throw new IOException("PUBLICATION_OUTSIDE_STRATEGY_CHANGED");
            requirePublication(namespace,publicationId,bundleSha,plan.snapshot("event_current"),commit);
            return existing; // Never move serving aliases back on a historical rerun.
        }
        freshness(plan,checks); lease.run();
        String statusTable=namespace+".publication_status";
        sql.execute("CREATE TABLE IF NOT EXISTS "+statusTable+" (publication_id varchar,gold_run_id varchar,snapshot_id bigint,"
            +"gold_table_name varchar,physical_gold_table_name varchar,committed_at_utc timestamp(6) with time zone,"
            +"verified_at_utc timestamp(6) with time zone,published_at_utc timestamp(6) with time zone,max_event_time_utc timestamp(6) with time zone,"
            +"event_count bigint,schema_version varchar,verify_status varchar,bundle_sha256 varchar,snapshot_bundle_json varchar,"
            +"scope_json varchar,serving_view varchar"+(operational ? ",coverage_mode varchar,publication_quality varchar,source_policy varchar,"
                +"research_ready boolean,ml_upstream_eligible boolean,event_cutoff_utc varchar,observation_cutoff_utc varchar,operational_policy_json varchar" : "")
            +") WITH (format='PARQUET',format_version=2)");
        String suffix=bundleSha.substring(0,24); var views=report.putObject("views");
        for(String name:GoldIcebergWriter.TABLES) {
            lease.run();String view=namespace+".v_"+suffix+"_"+name;views.put(name,view);
            sql.execute("CREATE OR REPLACE VIEW "+view+" AS SELECT *"+(operational ? ", 'OBSERVED_AS_OF' AS coverage_mode, 'PROVISIONAL' AS publication_quality, false AS research_ready, false AS ml_upstream_eligible" : "")+" FROM "+plan.ref(name)
                +" WHERE EXISTS (SELECT 1 FROM "+statusTable+" WHERE publication_id="+quote(publicationId)+" AND verify_status='PASSED')");
        }
        String naturalView=namespace+".v_"+suffix+"_earthquake_event_current";views.put("earthquake_event_current",naturalView);
        sql.execute("CREATE OR REPLACE VIEW "+naturalView+" AS SELECT * FROM "+views.path("event_current").asText()
                +" WHERE is_natural_earthquake AND is_in_study_area");
        long naturalCount=scalar("SELECT count(*) FROM "+plan.ref("event_current")+" WHERE is_natural_earthquake AND is_in_study_area");
        String now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString(); long existing=scalar("SELECT count(*) FROM "+statusTable+" WHERE publication_id="+quote(publicationId));
        if(existing==0) {
            freshness(plan,checks);lease.run();
            String committed=commit.path("tables").path("event_current").path("committed_at_utc").asText();Instant.parse(committed);
            sql.execute("INSERT INTO "+statusTable+" SELECT "+quote(publicationId)+","+quote(goldRun)+","+plan.snapshot("event_current")+","
                +quote(namespace+".earthquake_event_current")+","+quote(plan.table("event_current"))+",from_iso8601_timestamp("+quote(committed)+")"
                +",from_iso8601_timestamp("+quote(now)+"),from_iso8601_timestamp("+quote(now)+"),max(event_time_utc),"+naturalCount+",'1.0','PASSED',"+quote(bundleSha)+","
                +quote(commit.path("tables").toString())+","+quote(commit.path("scope").toString())+","+quote(naturalView)
                +(operational ? ",'OBSERVED_AS_OF','PROVISIONAL','USGS_OPERATIONAL_WITH_JMA_HISTORICAL_BASELINE',false,false,"
                    +quote(commit.path("operational_policy").path("event_cutoff_utc").asText())+","
                    +quote(commit.path("operational_policy").path("observation_cutoff_utc").asText())+","+quote(commit.path("operational_policy").toString()) : "")
                +" FROM "+plan.ref("event_current"));
        }
        requirePublication(namespace,publicationId,bundleSha,plan.snapshot("event_current"),commit);
        checked(checks,"serving_count","SELECT count(*) FROM "+naturalView,naturalCount);
        // Single natural/ROI alias changes only after the immutable bundle and PASSED row exist.
        lease.run();sql.execute("CREATE OR REPLACE VIEW "+namespace+".earthquake_event_current AS SELECT * FROM "+naturalView);
        checked(checks,"stable_serving_count","SELECT count(*) FROM "+namespace+".earthquake_event_current",naturalCount);
        report.put("verify_status","PASSED");report.put("published",true);report.put("event_count",naturalCount);
        report.put("verified_at_utc",now);report.set("scope",commit.path("scope"));
        byte[] bytes=JSON.writeValueAsBytes(report);lease.run();store.put(journal,bytes,"application/json");
        if(!Arrays.equals(bytes,store.read(journal))) throw new IOException("PUBLICATION_READBACK_FAILED");
        return JSON.readTree(bytes);
    }
    private void freshness(GoldVerificationPlan plan,ObjectNode checks) throws Exception {
        for(String name:GoldIcebergWriter.TABLES) checked(checks,"current:"+name,
            "SELECT snapshot_id FROM "+plan.metadata(name,"refs")+" WHERE name='main'",plan.snapshot(name));
    }
    private void requirePublication(String ns,String id,String sha,long snapshot,JsonNode commit) throws Exception {
        if(scalar("SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(id))!=1
                || scalar("SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(id)
                    +" AND verify_status='PASSED' AND snapshot_id="+snapshot+" AND bundle_sha256="+quote(sha))!=1)
            throw new IOException("PUBLICATION_ROW_CONFLICT");
        if(operational && scalar("SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(id)
                +" AND coverage_mode='OBSERVED_AS_OF' AND publication_quality='PROVISIONAL' AND NOT research_ready AND NOT ml_upstream_eligible"
                +" AND operational_policy_json="+quote(commit.path("operational_policy").toString()))!=1)
            throw new IOException("ASOF_PUBLICATION_ROW_POLICY_CONFLICT");
    }
    private void policy(JsonNode commit) {
        if(bucketedOutside && !commit.path("namespace").asText().matches("iceberg\\.dat03_catalog_[a-f0-9]{32}"))
            throw new IllegalArgumentException("DAT03_ISOLATED_CATALOG_REQUIRED");
        if(operational) OperationalGoldPolicy.validate(commit.path("namespace").asText(),commit.path("operational_policy"));
        else {
            OperationalGoldPolicy.researchOnly(commit.path("namespace").asText());
            if(commit.has("operational_policy")) throw new IllegalArgumentException("ASOF_EXPLICIT_PUBLISHER_REQUIRED");
        }
    }
    private void outside(GoldVerificationPlan plan,ObjectNode checks) throws Exception {
        for(String name:List.of("event_current","event_source_bridge")) {
            if(!bucketedOutside) {checked(checks,"outside:"+name,plan.outsideScope(name),0);continue;}
            for(char bucket:"0123456789abcdef".toCharArray())
                checked(checks,"outside:"+name+":"+bucket,plan.outsideScopeBucket(name,String.valueOf(bucket)),0);
            // This is an explicitly derived assertion, not a fabricated SQL readback.
            checks.putObject("outside:"+name).put("kind","EXACT_EXHAUSTIVE_BUCKET_SUM")
                    .put("query_count",16).put("actual",0).put("expected",0);
        }
    }
    private void operationalChecks(JsonNode commit,GoldVerificationPlan plan,ObjectNode checks) throws Exception {
        if(!operational) return;
        var policy=commit.path("operational_policy");
        for(String name:GoldIcebergWriter.TABLES) checked(checks,"policy:"+name,
            "SELECT count(*) FROM "+plan.metadata(name,"snapshots")+" WHERE snapshot_id="+plan.snapshot(name)
            +" AND summary['"+OperationalGoldPolicy.PROPERTY+"']="+quote(OperationalGoldPolicy.sha(policy)),1);
        checked(checks,"operational:source_cutoff","SELECT count(*) FROM "+plan.ref("event_current")
            +" WHERE canonical_source_system<>'USGS' OR NOT has_usgs OR has_jma OR source_count<>1 OR NOT is_in_study_area"
            +" OR event_time_utc>=from_iso8601_timestamp("+quote(policy.path("event_cutoff_utc").asText())+")",0);
        var covered=new ArrayList<String>();
        policy.path("month_coverage").fields().forEachRemaining(e->{
            java.time.YearMonth.parse(e.getKey());
            java.time.Instant.parse(e.getValue().path("event_start_utc").asText());
            java.time.Instant.parse(e.getValue().path("event_end_utc_exclusive").asText());
            covered.add("(event_time_utc>=from_iso8601_timestamp("+quote(e.getValue().path("event_start_utc").asText())
                +") AND event_time_utc<from_iso8601_timestamp("+quote(e.getValue().path("event_end_utc_exclusive").asText())+"))");
        });
        checked(checks,"operational:coverage","SELECT count(*) FROM "+plan.ref("event_current")+" WHERE NOT ("+String.join(" OR ",covered)+")",0);
    }
    private void checked(ObjectNode checks,String name,String query,long expected) throws Exception {
        lease.run();long actual=scalar(query);var check=checks.putObject(name);check.put("sql",query);check.put("expected",expected);check.put("actual",actual);
        if(actual!=expected) throw new IOException("TRINO_BLOCKER_FAILED:"+name);
    }
    private long scalar(String query) throws Exception {
        var result=sql.execute(query);
        if(result.rows().size()!=1 || result.rows().get(0).size()!=1 || !result.rows().get(0).get(0).isIntegralNumber())
            throw new IOException("TRINO_SCALAR_REQUIRED");
        return result.rows().get(0).get(0).asLong();
    }
}
