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
    public GoldPublisher(Sql sql, SilverObjectStore store, Runnable lease) {
        this.sql=Objects.requireNonNull(sql);this.store=Objects.requireNonNull(store);this.lease=Objects.requireNonNull(lease);
    }
    public JsonNode publish(String goldRun, String expectedNamespace, String expectedIdentity) throws Exception {
        lease.run();
        if(goldRun==null || !goldRun.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) throw new IllegalArgumentException("EXACT_GOLD_RUN_REQUIRED");
        byte[] commitBytes=store.read("gold-commits/"+goldRun+"/commit.json");
        JsonNode commit=JSON.readTree(commitBytes);
        if(!goldRun.equals(commit.path("gold_run_id").asText()) || !Objects.equals(expectedNamespace,commit.path("namespace").asText())
                || !Objects.equals(expectedIdentity,commit.path("identity_sha256").asText())) throw new IOException("COMMIT_PIN_MISMATCH");
        var plan=new GoldVerificationPlan(commit); String namespace=plan.namespace();
        String bundleSha=SilverBundlePublisher.sha(commitBytes), publicationId="pub_"+bundleSha;
        String journal="gold-verification/"+goldRun+"/publication.json";
        ObjectNode report=JSON.createObjectNode();report.put("task","GLD-04");report.put("engine","TRINO");
        report.put("publication_id",publicationId); report.put("bundle_sha256",bundleSha);report.put("identity_sha256",expectedIdentity);
        report.put("gold_run_id",goldRun);report.put("namespace",namespace);report.set("snapshot_bundle",commit.path("tables"));
        var checks=report.putObject("checks");
        // First verify all exact snapshots. No view/metadata mutation precedes these blockers.
        for(String name:GoldIcebergWriter.TABLES) {
            var shape=sql.execute("SELECT * FROM "+plan.ref(name)+" LIMIT 0");
            Map<String,String> actual=new TreeMap<>();shape.columns().forEach(c->actual.put(c.path("name").asText(),c.path("type").asText()));
            if(!actual.equals(plan.expectedTypes(name))) throw new IOException("TRINO_SCHEMA_MISMATCH:"+name);
            checked(checks,"count:"+name,"SELECT count(*) FROM "+plan.ref(name),plan.count(name));
            checked(checks,"identity:"+name,"SELECT count(*) FROM "+plan.metadata(name,"snapshots")+" WHERE snapshot_id="+plan.snapshot(name)
                +" AND summary['gold.operation_id']="+quote(goldRun)+" AND summary['gold.identity_sha256']="+quote(expectedIdentity),1);
        }
        for(var query:plan.blockers().entrySet()) checked(checks,query.getKey(),query.getValue(),0);
        checked(checks,"outside:event_current",plan.outsideScope("event_current"),0);
        checked(checks,"outside:event_source_bridge",plan.outsideScope("event_source_bridge"),0);
        if(store.exists(journal)) {
            JsonNode existing=JSON.readTree(store.read(journal));
            if(!publicationId.equals(existing.path("publication_id").asText()) || !"PASSED".equals(existing.path("verify_status").asText())
                    || !existing.path("published").asBoolean(false)) throw new IOException("PUBLICATION_JOURNAL_CONFLICT");
            requirePublication(namespace,publicationId,bundleSha,plan.snapshot("event_current"));
            return existing; // Never move serving aliases back on a historical rerun.
        }
        freshness(plan,checks); lease.run();
        String statusTable=namespace+".publication_status";
        sql.execute("CREATE TABLE IF NOT EXISTS "+statusTable+" (publication_id varchar,gold_run_id varchar,snapshot_id bigint,"
            +"gold_table_name varchar,physical_gold_table_name varchar,committed_at_utc timestamp(6) with time zone,"
            +"verified_at_utc timestamp(6) with time zone,published_at_utc timestamp(6) with time zone,max_event_time_utc timestamp(6) with time zone,"
            +"event_count bigint,schema_version varchar,verify_status varchar,bundle_sha256 varchar,snapshot_bundle_json varchar,"
            +"scope_json varchar,serving_view varchar) WITH (format='PARQUET',format_version=2)");
        String suffix=bundleSha.substring(0,24); var views=report.putObject("views");
        for(String name:GoldIcebergWriter.TABLES) {
            lease.run();String view=namespace+".v_"+suffix+"_"+name;views.put(name,view);
            sql.execute("CREATE OR REPLACE VIEW "+view+" AS SELECT * FROM "+plan.ref(name)
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
                +quote(commit.path("tables").toString())+","+quote(commit.path("scope").toString())+","+quote(naturalView)+" FROM "+plan.ref("event_current"));
        }
        requirePublication(namespace,publicationId,bundleSha,plan.snapshot("event_current"));
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
    private void requirePublication(String ns,String id,String sha,long snapshot) throws Exception {
        if(scalar("SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(id))!=1
                || scalar("SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(id)
                    +" AND verify_status='PASSED' AND snapshot_id="+snapshot+" AND bundle_sha256="+quote(sha))!=1)
            throw new IOException("PUBLICATION_ROW_CONFLICT");
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
