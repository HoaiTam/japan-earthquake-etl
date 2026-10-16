package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Replays exact baseline inputs + delta, never overwrites Silver/Gold partitions or discovers latest inputs. */
public final class FullMonthStateResolver {
    @FunctionalInterface public interface BaselineProbe { Map<String,Long> current() throws Exception; }
    private final BronzeObjectStore bronze;
    private final SilverObjectStore silver;
    private final FullMonthBronzeReader reader;
    private final BaselineProbe probe;
    private final Runnable lease;
    public FullMonthStateResolver(BronzeObjectStore bronze, SilverObjectStore silver, List<JmaArchiveEntry> inventory,
            BaselineProbe probe, Runnable lease) {
        this.bronze=bronze; this.silver=silver; this.reader=new FullMonthBronzeReader(bronze,inventory);
        this.probe=probe; this.lease=lease;
    }
    public static String root(String run) { SilverBundlePublisher.root(run); return "full-month/"+run; }

    public ObjectNode resolve(SparkSession spark, JsonNode request) throws Exception {
        validate(request); lease.run();
        String run=text(request,"run_id"), requestSha=digest(request), root=root(run);
        Map<String,Long> expected=snapshots(request.path("baseline_snapshots"));
        require(probe.current().equals(expected), "STALE_GOLD_BASELINE");
        if (silver.exists(root+"/identity.sha256"))
            require(Arrays.equals(silver.read(root+"/identity.sha256"),requestSha.getBytes(StandardCharsets.US_ASCII)), "RESOLVER_IDENTITY_CONFLICT");
        // Even a completed rerun re-verifies every pinned baseline/input and current snapshots.
        var registered=new TreeSet<String>(); var affected=new TreeSet<>(months(request.path("target_months")));
        var oldInputs=new ArrayList<FullMonthBronzeReader.Input>();
        JsonNode baseline=request.path("baseline");
        if (!baseline.isNull()) {
            var previous=verify(text(baseline,"run_id"),text(baseline,"registry_sha256"));
            JsonNode registry=JSON.readTree(silver.read(root(text(baseline,"run_id"))+"/registry.json"));
            require(text(request,"namespace").equals(registry.path("namespace").asText())
                    && text(request,"config_version").equals(registry.path("config_version").asText())
                    && !utc(registry,"source_cutoff_utc").isAfter(utc(request,"source_cutoff_utc")), "BASELINE_POLICY_OR_CUTOFF_MISMATCH");
            registered.addAll(months(registry.path("complete_months")));
            for (var pin:registry.path("active_inputs")) {
                oldInputs.add(reader.read(pin,utc(request,"source_cutoff_utc")));
                require(oldInputs.size()<=64 && oldInputs.stream().mapToLong(i->i.raw().length).sum()<=128L*1024*1024,
                        "BOUNDED_PARENT_INPUT_BYTES_REQUIRED");
            }
            if (expected.values().stream().anyMatch(id->id!=0)) {
                byte[] commitBytes=silver.read(text(baseline,"commit_key"));
                require(sha(commitBytes).equals(text(baseline,"commit_sha256")), "BASELINE_COMMIT_PIN_MISMATCH");
                var commit=JSON.readTree(commitBytes);
                require("COMMITTED".equals(commit.path("status").asText())
                        && text(request,"namespace").equals(commit.path("namespace").asText())
                        && previous.path("silver_bundle_sha256").asText().equals(commit.path("scope").path("silver_bundle_sha256").asText()), "BASELINE_COMMIT_LINEAGE_REQUIRED");
                for(var table:expected.entrySet()) require(commit.path("tables").path(table.getKey()).path("snapshot_id").isIntegralNumber()
                        && commit.path("tables").path(table.getKey()).path("snapshot_id").asLong()==table.getValue(), "BASELINE_COMMIT_SNAPSHOT_MISMATCH");
            }
        } else require(expected.values().stream().allMatch(id->id==0), "EXISTING_GOLD_REQUIRES_PINNED_REGISTRY");

        var active=new TreeMap<String,FullMonthBronzeReader.Input>();
        oldInputs.forEach(i->active.put(i.pin().path("manifest_uri").asText(),i));
        var deltaIds=new HashSet<String>(); var seen=new HashSet<String>();
        for(var pin:request.path("bronze_inputs")) {
            String uri=text(pin,"manifest_uri"); require(seen.add(uri), "DUPLICATE_INPUT_PIN");
            var incoming=reader.read(pin,utc(request,"source_cutoff_utc")); deltaIds.add(text(incoming.manifest(),"manifest_id"));
            if(active.containsKey(uri)) require(active.get(uri).pin().equals(pin), "SAME_URI_DIFFERENT_PIN");
            if("JMA_BULLETIN".equals(pin.path("source_system").asText())) {
                var sameSlot=active.values().stream().filter(i->"JMA_BULLETIN".equals(i.pin().path("source_system").asText())
                        && i.pin().path("year").equals(pin.path("year")) && i.pin().path("segment").equals(pin.path("segment"))).toList();
                for(var prior:sameSlot) {
                    int order=new JmaReleaseComparator().compare(prior.pin().path("catalog_release").asText(),pin.path("catalog_release").asText());
                    if(!prior.pin().path("sha256").equals(pin.path("sha256"))) {
                        require(order<0, "JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT");
                        // Compare ALL removed/added native records, including outside requested months.
                        // A moved JMA record changes its key: include both months without guessing identity.
                        affected.addAll(FullMonthBronzeReader.changedJmaMonths(prior,incoming));
                    } else require(order<=0, "OLDER_JMA_RELEASE_REJECTED");
                    active.remove(prior.pin().path("manifest_uri").asText());
                }
            }
            active.put(uri,incoming);
            require(active.size()<=64 && active.values().stream().mapToLong(i->i.raw().length).sum()<=128L*1024*1024,
                    "BOUNDED_ACTIVE_INPUT_BYTES_REQUIRED");
        }
        require(!active.isEmpty() && active.size()<=64, "BOUNDED_ACTIVE_INPUTS_REQUIRED");
        require(active.values().stream().mapToLong(i->i.raw().length).sum()<=128L*1024*1024
                && oldInputs.stream().mapToLong(i->i.raw().length).sum()<=128L*1024*1024, "BOUNDED_TOTAL_INPUT_BYTES_REQUIRED");
        var parseMonths=new TreeSet<>(registered); parseMonths.addAll(affected);
        // USGS keeps stable source keys when origin time changes. Inspect both old/new
        // timestamps before JMA selection so a moved event expands the month automatically.
        var usgsInputs=active.values().stream().filter(i->"USGS".equals(i.pin().path("source_system").asText())).toList();
        for(var observation:reader.parse(usgsInputs,parseMonths)) parseMonths.add(month(observation.eventTimeUtc()));
        require(parseMonths.size()<=24, "BOUNDED_REBUILD_MONTHS_REQUIRED");
        var parsed=reader.parseWithReport(new ArrayList<>(active.values()),parseMonths);
        var observations=parsed.observations();
        var oldObservations=oldInputs.isEmpty()?List.<SilverObservation>of():reader.parse(oldInputs,parseMonths);
        var before=new SourceDedupTransformer().deduplicate(oldObservations);
        var after=new SourceDedupTransformer().deduplicate(observations);
        if(!baseline.isNull()) {
            var oldByKey=currentKeys(before.currentObservations()); var newByKey=currentKeys(after.currentObservations());
            var keys=new TreeSet<>(oldByKey.keySet()); keys.addAll(newByKey.keySet());
            for(String k:keys) {
                var old=oldByKey.get(k); var now=newByKey.get(k);
                boolean changed=old==null || now==null || !old.sourceObservationId().equals(now.sourceObservationId());
                if(changed && ((old!=null && registered.contains(month(old.eventTimeUtc())))
                        || (now!=null && (registered.contains(month(now.eventTimeUtc())) || deltaIds.contains(now.bronzeManifestId()))))) {
                    if(old!=null) affected.add(month(old.eventTimeUtc())); if(now!=null) affected.add(month(now.eventTimeUtc()));
                }
            }
        }
        Instant processed=utc(request,"processed_at_utc");
        var config=SilverLinkConfig.defaultConfig();
        var oldLinks=SilverEntityResolver.resolve(before.currentObservations(),config,processed);
        var newLinks=SilverEntityResolver.resolve(after.currentObservations(),config,processed);
        var oldMonths=monthsById(before.currentObservations()); var newMonths=monthsById(after.currentObservations());
        closeBothGraphs(affected,oldMonths,oldLinks.sourceLinks(),newMonths,newLinks.sourceLinks());
        // New closure may reach a month whose JMA rows were not parsed. Never certify a partial expansion.
        require(parseMonths.containsAll(affected), "MONTH_EXPANSION_REQUIRES_EXPLICIT_REPLAN");
        require(affected.size()<=24, "BOUNDED_AFFECTED_MONTHS_REQUIRED");
        var coverage=coverage(new ArrayList<>(active.values()),affected);
        var allComplete=new TreeSet<>(registered); allComplete.addAll(affected);
        var registryCoverage=coverage(new ArrayList<>(active.values()),allComplete);
        var output=after.allObservations().stream().filter(o->affected.contains(month(o.eventTimeUtc()))).toList();
        var outputIds=new HashSet<String>(); output.forEach(o->outputIds.add(o.sourceObservationId()));
        var memberships=newLinks.canonicalMemberships().stream().filter(m->outputIds.contains(m.sourceObservationId())).toList();
        var links=newLinks.sourceLinks().stream().filter(l->outputIds.contains(l.leftObservationId()) && outputIds.contains(l.rightObservationId())).toList();
        var lineage=new TreeMap<String,String>(); active.values().forEach(i->lineage.put(i.manifest().path("manifest_id").asText(),i.manifest().path("run_id").asText()));
        // Reuse existing quality + source dedup + linking; no new canonical/key policy.
        var integrated=new SilverMultiSourceIntegrationRunner().run(run,output,List.of(),config,null,false,processed,lineage);
        require(integrated.isPublishable(), "MONTHLY_QUALITY_OR_RECONCILIATION_BLOCKED");
        require(new HashSet<>(integrated.canonicalMemberships()).equals(new HashSet<>(memberships)), "CANONICAL_CLOSURE_MISMATCH");
        require(new HashSet<>(integrated.sourceLinks()).equals(new HashSet<>(links)), "SOURCE_LINK_CLOSURE_MISMATCH");
        var frame=spark.createDataFrame(integrated.allObservations().stream().map(SilverObservation::toRow).toList(),SilverSchemas.OBSERVATION_SCHEMA);
        require(frame.count()==output.size() && frame.select("source_observation_id").distinct().count()==output.size(), "OBSERVATION_GRAIN_MISMATCH");
        var report=JSON.createObjectNode();
        report.put("input_unique_revisions",observations.size()); report.put("output_revisions",output.size());
        report.put("selected_input_records",parsed.selectedRows());
        report.put("repeated_input_observation_ids",parsed.selectedRows()-observations.size());
        report.put("revisions_outside_affected_months",observations.size()-output.size());
        report.set("sources",parsed.sources());
        report.put("current",integrated.currentObservations().size()); report.put("history",integrated.dedupResult().historyObservations().size());
        report.put("canonical",integrated.canonicalMemberships().stream().map(SilverCanonicalMembership::canonicalEventId).distinct().count());
        report.put("memberships",integrated.canonicalMemberships().size()); report.put("links",integrated.sourceLinks().size());
        report.put("balanced",integrated.reconciliationReport().isReconciliationBalanced());
        var obsById=new HashMap<String,SilverObservation>(); integrated.currentObservations().forEach(o->obsById.put(o.sourceObservationId(),o));
        var monthCounts=report.putObject("canonical_by_month"); var emptyMonths=report.putArray("explicit_empty_months");
        for(String m:affected) {
            long count=integrated.canonicalMemberships().stream().filter(row->"PRIMARY".equals(row.membershipStatus())
                    && m.equals(month(obsById.get(row.sourceObservationId()).eventTimeUtc()))).count();
            monthCounts.put(m,count); if(count==0) emptyMonths.add(m);
        }
        var context=JSON.createObjectNode(); context.put("quality_passed",true); context.put("reconciliation_balanced",true);
        context.put("resolver_version",VERSION); context.put("request_sha256",requestSha);
        context.set("coverage",coverage); context.set("reconciliation",report);
        context.set("affected_months",JSON.valueToTree(affected)); context.set("link_config",JSON.valueToTree(config));
        lease.run(); require(probe.current().equals(expected), "STALE_GOLD_BASELINE");
        immutable(root+"/identity.sha256",requestSha.getBytes(StandardCharsets.US_ASCII));
        var publisher=new SilverBundlePublisher(silver);
        var written=publisher.publish(new SilverWriteRequest(run,integrated.allObservations(),List.of(),integrated.sourceLinks(),
                integrated.canonicalMemberships(),processed,false),context);
        byte[] bundleBytes=silver.read(SilverBundlePublisher.root(run)+"/manifest.json");
        publisher.verify(run); registered.addAll(affected);
        var registry=JSON.createObjectNode(); registry.put("resolver_version",VERSION); registry.put("run_id",run);
        registry.put("namespace",text(request,"namespace")); registry.put("config_version",text(request,"config_version"));
        registry.put("source_cutoff_utc",text(request,"source_cutoff_utc")); registry.put("request_sha256",requestSha);
        registry.set("complete_months",JSON.valueToTree(registered)); registry.set("active_inputs",JSON.valueToTree(active.values().stream().map(FullMonthBronzeReader.Input::pin).toList()));
        registry.set("parent",baseline.deepCopy()); registry.put("silver_bundle_sha256",sha(bundleBytes)); registry.set("coverage",registryCoverage);
        byte[] registryBytes=JSON.writeValueAsBytes(canonical(registry)); immutable(root+"/registry.json",registryBytes);
        var receipt=JSON.createObjectNode(); receipt.put("resolver_version",VERSION); receipt.put("run_id",run);
        receipt.put("status","FullMonthReady"); receipt.put("scope_completeness","COMPLETE_MONTHS"); receipt.put("gold_published",false);
        receipt.put("request_sha256",requestSha); receipt.put("registry_uri",silver.uriForKey(root+"/registry.json")); receipt.put("registry_sha256",sha(registryBytes));
        receipt.put("silver_run_id",run); receipt.put("silver_bundle_sha256",sha(bundleBytes));
        receipt.put("silver_bundle_uri",silver.uriForKey(SilverBundlePublisher.root(run)+"/manifest.json"));
        receipt.set("affected_months",JSON.valueToTree(affected)); receipt.set("coverage",coverage); receipt.set("reconciliation",report);
        receipt.set("baseline_snapshots",JSON.valueToTree(expected)); receipt.set("gold_request",goldRequest(request,affected,sha(bundleBytes)));
        byte[] receiptBytes=JSON.writeValueAsBytes(canonical(receipt));
        lease.run(); require(probe.current().equals(expected), "STALE_GOLD_BASELINE");
        immutable(root+"/receipt.json",receiptBytes); immutable(root+"/_SUCCESS",sha(receiptBytes).getBytes(StandardCharsets.US_ASCII));
        return verify(run,sha(registryBytes));
    }

    public ObjectNode verify(String run,String registrySha) throws IOException {
        String root=root(run); byte[] bytes=silver.read(root+"/receipt.json");
        require(Arrays.equals(silver.read(root+"/_SUCCESS"),sha(bytes).getBytes(StandardCharsets.US_ASCII)), "FULL_MONTH_MARKER_MISMATCH");
        var receipt=(ObjectNode)JSON.readTree(bytes); byte[] registry=silver.read(root+"/registry.json");
        require(VERSION.equals(receipt.path("resolver_version").asText()) && run.equals(receipt.path("run_id").asText())
                && "FullMonthReady".equals(receipt.path("status").asText()) && sha(registry).equals(registrySha)
                && registrySha.equals(receipt.path("registry_sha256").asText())
                && receipt.path("request_sha256").asText().equals(new String(silver.read(root+"/identity.sha256"),StandardCharsets.US_ASCII)), "FULL_MONTH_RECEIPT_PIN_MISMATCH");
        new SilverBundlePublisher(silver).verify(run);
        require(sha(silver.read(SilverBundlePublisher.root(run)+"/manifest.json")).equals(receipt.path("silver_bundle_sha256").asText()), "FULL_MONTH_SILVER_PIN_MISMATCH");
        return receipt;
    }
    private void immutable(String key,byte[] bytes) throws IOException {
        lease.run();
        if(silver.exists(key)) require(Arrays.equals(silver.read(key),bytes), "IMMUTABLE_MONTH_STATE_CONFLICT");
        else silver.put(key,bytes,"application/json");
        require(Arrays.equals(silver.read(key),bytes), "MONTH_STATE_READBACK_FAILED");
    }
    private static Map<String,SilverObservation> currentKeys(List<SilverObservation> rows) {
        var out=new TreeMap<String,SilverObservation>(); rows.forEach(o->out.put(o.sourceSystem()+":"+o.sourceRecordKey(),o)); return out;
    }
    private static Map<String,String> monthsById(List<SilverObservation> observations) {
        var byId=new HashMap<String,String>(); observations.forEach(o->byId.put(o.sourceObservationId(),month(o.eventTimeUtc()))); return byId;
    }
    static void closeBothGraphs(Set<String> affected,Map<String,String> oldMonths,List<SilverSourceLink> oldLinks,
            Map<String,String> newMonths,List<SilverSourceLink> newLinks) {
        int previous;
        do { previous=affected.size(); closeLinks(affected,oldMonths,oldLinks); closeLinks(affected,newMonths,newLinks); }
        while(previous!=affected.size());
    }
    private static void closeLinks(Set<String> affected,Map<String,String> byId,List<SilverSourceLink> links) {
        boolean changed;
        do { changed=false; for(var link:links) {
            String left=byId.get(link.leftObservationId()), right=byId.get(link.rightObservationId());
            if(affected.contains(left)||affected.contains(right)) { changed|=affected.add(left); changed|=affected.add(right); }
        } } while(changed);
    }
    public static ArrayNode coverage(List<FullMonthBronzeReader.Input> inputs,Set<String> months) throws IOException {
        var proof=JSON.createArrayNode();
        for(String month:months) for(String source:List.of("USGS","JMA_BULLETIN")) {
            Instant from=start(month).minusSeconds(HALO_SECONDS), to=end(month).plusSeconds(HALO_SECONDS);
            var row=proof.addObject(); row.put("month",month); row.put("source_system",source);
            if("USGS".equals(source) && !to.isAfter(USGS_START)) { row.put("coverage","NOT_APPLICABLE_BEFORE_USGS_SEED"); continue; }
            if("USGS".equals(source) && from.isBefore(USGS_START)) from=USGS_START;
            Instant covered=from; var used=row.putArray("manifest_uris");
            var windows=inputs.stream().filter(i->source.equals(i.pin().path("source_system").asText())).sorted(Comparator.comparing(FullMonthBronzeReader.Input::start)).toList();
            for(var i:windows) if(!i.start().isAfter(covered) && i.end().isAfter(covered)) {
                covered=i.end(); used.add(i.pin().path("manifest_uri").asText()); if(!covered.isBefore(to)) break;
            }
            require(!covered.isBefore(to), "MISSING_FULL_MONTH_SOURCE_COVERAGE:"+month+":"+source);
            row.put("coverage","COMPLETE"); row.put("required_start_utc",from.toString()); row.put("required_end_utc",to.toString());
        }
        return proof;
    }
    private ObjectNode goldRequest(JsonNode request,SortedSet<String> affected,String sha) throws IOException {
        var out=JSON.createObjectNode();
        for(String k:List.of("dag_id","namespace","processed_at_utc","code_version","baseline_snapshots")) out.set(k,request.path(k).deepCopy());
        out.put("silver_run_id",text(request,"run_id")); out.put("silver_bundle_sha256",sha);
        out.put("scope_completeness","COMPLETE_MONTHS"); out.set("affected_months",JSON.valueToTree(affected));
        var run=out.putObject("run_context"); run.put("run_id",text(request,"run_id"));
        run.put("window_start_utc",start(affected.first()).toString()); run.put("window_end_utc",end(affected.last()).toString());
        run.put("processing_date",utc(request,"processed_at_utc").atZone(ZoneOffset.UTC).toLocalDate().toString()); run.put("is_backfill",true);
        run.put("config_version",text(request,"config_version")); run.putArray("input_manifest_uris").add(silver.uriForKey(SilverBundlePublisher.root(text(request,"run_id"))+"/manifest.json"));
        return out;
    }
}
