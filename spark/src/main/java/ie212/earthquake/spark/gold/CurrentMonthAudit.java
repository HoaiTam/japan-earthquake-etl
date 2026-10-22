package ie212.earthquake.spark.gold;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.gold.GoldVerificationPlan.quote;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Read-only independent acceptance. No capture, Spark writes, publication or lease mutation. */
public final class CurrentMonthAudit {
    private CurrentMonthAudit() { }
    public static ObjectNode verify(JsonNode request,SilverObjectStore raw,CurrentMonthSilverResolver resolver,
            GoldPublisher.Sql sql) throws Exception {
        CurrentMonthGold.validate(request);String op=text(request,"operation_id"),ns=text(request,"namespace");
        var store=new CurrentMonthSilverResolver.OperationalStore(raw);
        var silver=resolver.verify(op,text(request,"registry_sha256"));
        String root="gold-state/"+op;byte[] bytes=store.read(root+"/receipt.json");
        require(Arrays.equals(store.read(root+"/_SUCCESS"),sha(bytes).getBytes(StandardCharsets.US_ASCII)),"ASOF_GOLD_MARKER_CHANGED");
        var receipt=JSON.readTree(bytes);
        require(JSON.readTree(store.read(root+"/request.json")).equals(canonical(request))
            && digest(request).equals(text(receipt,"request_sha256")) && op.equals(text(receipt,"operation_id"))
            && "Published".equals(text(receipt,"status")) && receipt.path("published").asBoolean(false),"ASOF_GOLD_RECEIPT_CHANGED");
        byte[] commitBytes=store.read("gold-commits/"+op+"/commit.json"),pubBytes=store.read("gold-verification/"+op+"/publication.json");
        require(sha(commitBytes).equals(text(receipt,"commit_sha256")) && sha(pubBytes).equals(text(receipt,"publication_sha256")),"ASOF_GOLD_JOURNAL_CHANGED");
        var commit=JSON.readTree(commitBytes);var pub=JSON.readTree(pubBytes);var policy=commit.path("operational_policy");
        OperationalGoldPolicy.validate(ns,policy);
        require(op.equals(text(commit,"gold_run_id")) && ns.equals(text(commit,"namespace"))
            && policy.equals(receipt.path("operational_policy")) && policy.equals(pub.path("operational_policy"))
            && text(request,"registry_sha256").equals(text(policy,"registry_sha256"))
            && sha(store.read(CurrentMonthSilverResolver.root(op)+"/receipt.json")).equals(text(policy,"silver_receipt_sha256"))
            && text(policy,"event_cutoff_utc").equals(text(silver,"event_cutoff_utc"))
            && text(policy,"observation_cutoff_utc").equals(text(silver,"observation_cutoff_utc"))
            && silver.path("reconciliation").equals(receipt.path("reconciliation")),"ASOF_AUDIT_CHAIN_CHANGED");
        String publication="pub_"+sha(commitBytes);
        require(op.equals(text(pub,"gold_run_id")) && ns.equals(text(pub,"namespace"))
            && publication.equals(text(pub,"publication_id")) && "PASSED".equals(text(pub,"verify_status"))
            && pub.path("published").asBoolean(false) && sha(commitBytes).equals(text(pub,"bundle_sha256"))
            && commit.path("tables").equals(pub.path("snapshot_bundle"))
            && commit.path("tables").equals(receipt.path("snapshot_bundle")),"ASOF_AUDIT_PUBLICATION_CHANGED");
        var report=new GoldPublisher(sql,store,()->{},true).verify(op,ns,text(commit,"identity_sha256"));
        var checks=(ObjectNode)report.path("checks");var plan=new GoldVerificationPlan(commit);
        for(String table:List.of("event_current","event_source_bridge"))
            checked(sql,checks,"silver_to_gold:"+table,"SELECT count(*) FROM "+plan.ref(table),silver.path("reconciliation").path("current").asLong(-1));
        long natural=pub.path("event_count").asLong(-1);require(natural>=0 && natural==receipt.path("event_count").asLong(-2),"ASOF_AUDIT_COUNT_CHANGED");
        checked(sql,checks,"natural_roi","SELECT count(*) FROM "+plan.ref("event_current")+" WHERE is_natural_earthquake AND is_in_study_area",natural);
        checked(sql,checks,"publication_row","SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(publication)
            +" AND gold_run_id="+quote(op)+" AND snapshot_id="+plan.snapshot("event_current")+" AND verify_status='PASSED' AND bundle_sha256="+quote(sha(commitBytes))
            +" AND event_count="+natural+" AND coverage_mode='OBSERVED_AS_OF' AND publication_quality='PROVISIONAL' AND NOT research_ready AND NOT ml_upstream_eligible"
            +" AND operational_policy_json="+quote(policy.toString())+" AND (SELECT count(*) FROM "+ns+".publication_status WHERE publication_id="+quote(publication)+")=1",1);
        checked(sql,checks,"versioned_serving","SELECT count(*) FROM "+ns+".v_"+sha(commitBytes).substring(0,24)+"_earthquake_event_current",natural);
        checked(sql,checks,"versioned_policy","SELECT count(*) FROM "+ns+".v_"+sha(commitBytes).substring(0,24)
            +"_earthquake_event_current WHERE coverage_mode<>'OBSERVED_AS_OF' OR publication_quality<>'PROVISIONAL' OR research_ready OR ml_upstream_eligible",0);
        var registry=JSON.readTree(store.read(CurrentMonthSilverResolver.root(op)+"/registry.json"));
        var result=JSON.createObjectNode();result.put("contract_version","current-month-audit-v1");result.put("operation_id",op);
        result.set("source_request",registry.path("request"));result.set("silver",silver);result.set("gold_request",canonical(request));
        result.set("gold",receipt);result.set("publication",pub);result.set("verification",report);
        result.put("read_only",true);result.put("status","PASSED");OperationalGoldPolicy.attach(result,policy);return result;
    }
    private static void checked(GoldPublisher.Sql sql,ObjectNode checks,String name,String query,long expected) throws Exception {
        var result=sql.execute(query);
        require(result.rows().size()==1 && result.rows().get(0).size()==1 && result.rows().get(0).get(0).isIntegralNumber()
            && result.rows().get(0).get(0).asLong()==expected,"ASOF_AUDIT_SQL_FAILED");
        checks.putObject(name).put("sql",query).put("actual",expected).put("expected",expected);
    }
}
