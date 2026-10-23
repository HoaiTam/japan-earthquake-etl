package ie212.earthquake.spark.gold;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Explicit operational commit -> exact Trino verification -> provisional publication. No scheduler mutation. */
public final class CurrentMonthGold {
    private final SilverObjectStore store;
    private final CurrentMonthSilverResolver silver;
    private final GoldPublisher.Sql sql;
    private final Runnable lease;
    public CurrentMonthGold(SilverObjectStore store,CurrentMonthSilverResolver silver,GoldPublisher.Sql sql,Runnable lease) {
        this.store=new CurrentMonthSilverResolver.OperationalStore(Objects.requireNonNull(store));
        this.silver=Objects.requireNonNull(silver);this.sql=Objects.requireNonNull(sql);this.lease=Objects.requireNonNull(lease);
    }
    public static void validate(JsonNode request) throws java.io.IOException {
        CurrentMonthContract.fields(request,Set.of("contract_version","operation_id","registry_sha256","namespace","baseline_publication","code_version"));
        require(OperationalGoldPolicy.VERSION.equals(text(request,"contract_version")),"ASOF_GOLD_PROTOCOL_REQUIRED");
        CurrentMonthSilverResolver.root(text(request,"operation_id"));
        require(text(request,"registry_sha256").matches("[a-f0-9]{64}") && text(request,"code_version").matches("[a-f0-9]{40}"),"ASOF_GOLD_EXACT_PINS_REQUIRED");
        require(text(request,"namespace").matches("[a-z][a-z0-9_]*\\."+OperationalGoldPolicy.SCHEMA),"ASOF_RESERVED_GOLD_NAMESPACE_REQUIRED");
        var parent=request.path("baseline_publication");
        if(!parent.isNull()) {
            CurrentMonthContract.fields(parent,Set.of("operation_id","publication_sha256"));
            CurrentMonthSilverResolver.root(text(parent,"operation_id"));
            require(!text(parent,"operation_id").equals(text(request,"operation_id"))
                && text(parent,"publication_sha256").matches("[a-f0-9]{64}"),"ASOF_GOLD_PARENT_PIN_REQUIRED");
        }
    }
    public ObjectNode execute(SparkSession spark,JsonNode request,Path work) throws Exception {
        validate(request); lease.run(); String op=text(request,"operation_id"),ns=text(request,"namespace");
        // Resolver revalidates the exact source selection, all active evidence, Silver bundle and reconciliation.
        var resolved=silver.verify(op,text(request,"registry_sha256"));
        var registry=JSON.readTree(store.read(CurrentMonthSilverResolver.root(op)+"/registry.json"));
        var sourceRequest=registry.path("request");
        String receiptSha=sha(store.read(CurrentMonthSilverResolver.root(op)+"/receipt.json"));
        var policy=CurrentMonthContract.flags(); policy.remove("published");
        policy.put("contract_version",OperationalGoldPolicy.VERSION);policy.put("registry_sha256",text(request,"registry_sha256"));
        policy.put("silver_receipt_sha256",receiptSha);
        policy.set("event_cutoff_utc",resolved.path("event_cutoff_utc"));policy.set("observation_cutoff_utc",resolved.path("observation_cutoff_utc"));
        policy.set("month_coverage",registry.path("month_coverage")); OperationalGoldPolicy.validate(ns,policy);
        Map<String,Long> bases=new TreeMap<>(); GoldIcebergWriter.TABLES.forEach(n->bases.put(n,0L));
        var parent=request.path("baseline_publication");
        if(parent.isNull()) require(sourceRequest.path("baseline").isNull(),"ASOF_GOLD_SILVER_BASELINE_MISMATCH");
        else {
            String previous=text(parent,"operation_id");
            byte[] bytes=store.read("gold-verification/"+previous+"/publication.json");
            require(sha(bytes).equals(text(parent,"publication_sha256")),"ASOF_GOLD_PARENT_PUBLICATION_CHANGED");
            var publication=JSON.readTree(bytes); var prior=publication.path("operational_policy");
            OperationalGoldPolicy.validate(ns,prior);
            require(previous.equals(text(publication,"gold_run_id")) && ns.equals(text(publication,"namespace"))
                && publication.path("published").asBoolean(false) && "PASSED".equals(text(publication,"verify_status"))
                && previous.equals(sourceRequest.path("baseline").path("operation_id").asText())
                && text(prior,"registry_sha256").equals(sourceRequest.path("baseline").path("registry_sha256").asText()),"ASOF_GOLD_SILVER_BASELINE_MISMATCH");
            byte[] commit=store.read("gold-commits/"+previous+"/commit.json");
            var old=JSON.readTree(commit);new GoldVerificationPlan(old);
            require(sha(commit).equals(text(publication,"bundle_sha256")) && old.path("operational_policy").equals(prior)
                && old.path("tables").equals(publication.path("snapshot_bundle")),"ASOF_GOLD_PARENT_COMMIT_CHANGED");
            for(String name:GoldIcebergWriter.TABLES) bases.put(name,old.path("tables").path(name).path("snapshot_id").asLong());
        }
        String root="gold-state/"+op;
        immutable(root+"/request.json",JSON.writeValueAsBytes(canonical(request)));
        var scopes=CurrentMonthContract.scopes(sourceRequest);var months=scopes.stream().map(CurrentMonthContract.Scope::month).toList();
        Instant cutoff=utc(policy,"event_cutoff_utc"),processed=utc(policy,"observation_cutoff_utc");
        var context=new GoldRunContext(op,scopes.get(0).start(),cutoff,cutoff.atOffset(ZoneOffset.UTC).toLocalDate(),false,
            OperationalGoldPolicy.VERSION,List.of(text(resolved,"silver_bundle_uri")));
        var gold=GoldInputReader.read(spark,store,op,text(resolved,"silver_bundle_sha256"),context,processed,work,months);
        // No missing/rejected or non-USGS current row may disappear between Silver and Gold.
        long expected=0;
        for(var it=registry.path("source_index").fields();it.hasNext();) if(months.contains(it.next().getValue().path("month").asText())) expected++;
        require(gold.canonicalEventCount()==expected && gold.bridgeRowCount()==expected,"ASOF_GOLD_RECONCILIATION_FAILED");
        var commit=new GoldIcebergWriter(spark,ns,store,lease,policy).commit(gold,
            new GoldCommitScope(context,text(resolved,"silver_bundle_sha256"),text(request,"code_version"),months,bases));
        var publisher=new GoldPublisher(sql,store,lease,true);
        var verification=publisher.verify(op,ns,text(commit,"identity_sha256"));
        var publication=publisher.publish(op,ns,text(commit,"identity_sha256"));
        var result=JSON.createObjectNode(); result.put("contract_version",OperationalGoldPolicy.VERSION);result.put("operation_id",op);
        result.put("status","Published");result.put("published",true); OperationalGoldPolicy.attach(result,policy);
        result.put("request_sha256",digest(request));result.put("silver_receipt_sha256",receiptSha);
        result.put("commit_sha256",sha(store.read("gold-commits/"+op+"/commit.json")));
        result.put("publication_sha256",sha(store.read("gold-verification/"+op+"/publication.json")));
        result.put("publication_uri",store.uriForKey("gold-verification/"+op+"/publication.json"));
        result.set("snapshot_bundle",commit.path("tables"));result.set("reconciliation",resolved.path("reconciliation"));
        result.set("verification",verification);result.put("event_count",publication.path("event_count").asLong());
        immutable(root+"/receipt.json",JSON.writeValueAsBytes(canonical(result)));
        immutable(root+"/_SUCCESS",sha(store.read(root+"/receipt.json")).getBytes(StandardCharsets.US_ASCII));
        return (ObjectNode)JSON.readTree(store.read(root+"/receipt.json"));
    }
    private void immutable(String key,byte[] bytes) throws Exception {
        lease.run(); if(store.exists(key)) require(Arrays.equals(bytes,store.read(key)),"ASOF_GOLD_IMMUTABLE_CONFLICT");
        else store.put(key,bytes,"application/json");
        require(Arrays.equals(bytes,store.read(key)),"ASOF_GOLD_READBACK_FAILED");
    }
}
