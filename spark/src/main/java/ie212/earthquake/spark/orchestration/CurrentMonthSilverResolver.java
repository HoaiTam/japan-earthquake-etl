package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.orchestration.CurrentMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Isolated immutable operational state. No Gold request, catalog writes or historical completeness proof. */
public final class CurrentMonthSilverResolver {
    private final SilverObjectStore store;
    private final CurrentMonthCapture capture;
    private final Clock clock;
    private final Runnable guard;
    public CurrentMonthSilverResolver(SilverObjectStore store,CurrentMonthCapture capture,Clock clock,Runnable guard) {
        this.store=new OperationalStore(Objects.requireNonNull(store)); this.capture=Objects.requireNonNull(capture);
        this.clock=Objects.requireNonNull(clock); this.guard=Objects.requireNonNull(guard);
    }
    public static String root(String op) { CurrentMonthCapture.root(op); return "state/"+op; }
    public ObjectNode resolve(SparkSession spark,JsonNode request) throws Exception {
        CurrentMonthContract.validate(request,clock); guard.run(); String op=operation(request), root=root(op);
        var selected=capture.verify(request); String requestSha=digest(request);
        if(store.exists(root+"/identity.sha256")) require(Arrays.equals(store.read(root+"/identity.sha256"),requestSha.getBytes(StandardCharsets.US_ASCII)),"ASOF_RESOLVER_IDENTITY_CONFLICT");
        if(store.exists(root+"/_SUCCESS")) {
            var existing=verify(op,null); require(requestSha.equals(text(existing,"request_sha256")),"ASOF_RESOLVER_IDENTITY_CONFLICT");
            return existing;
        }
        var priorPins=new TreeMap<String,JsonNode>(); var coverage=JSON.createObjectNode();
        var parent=request.path("baseline");
        if(!parent.isNull()) {
            String previous=text(parent,"operation_id"); verify(previous,text(parent,"registry_sha256"));
            var registry=JSON.readTree(store.read(root(previous)+"/registry.json"));
            require(!utc(registry,"event_cutoff_utc").isAfter(cutoff(request))
                    && !utc(registry,"observation_cutoff_utc").isAfter(utc(selected,"observation_cutoff_utc")),"ASOF_PARENT_CUTOFF_REGRESSION");
            for(var pin:registry.path("active_inputs")) priorPins.put(text(pin,"manifest_uri"),pin);
            coverage.setAll((ObjectNode)registry.path("month_coverage"));
        }
        var old=parse(priorPins.values()); var incomingPins=new TreeMap<String,JsonNode>();
        for(var part:selected.path("parts")) incomingPins.put(text(part.path("pin"),"manifest_uri"),part.path("pin"));
        var incoming=parse(incomingPins.values());
        var before=new SourceDedupTransformer().deduplicate(old.observations());
        var affected=new TreeSet<String>(); scopes(request).forEach(s->affected.add(s.month()));
        var newByKey=new HashMap<String,SilverObservation>();
        for(var row:incoming.observations()) {
            var prior=newByKey.putIfAbsent(row.sourceRecordKey(),row);
            require(prior==null || prior.rawRecordHash().equals(row.rawRecordHash()),"ASOF_SOURCE_CHANGED_DURING_CAPTURE");
        }
        for(var prior:before.currentObservations()) {
            var next=newByKey.get(prior.sourceRecordKey());
            if(affected.contains(month(prior.eventTimeUtc()))) require(next!=null,"ASOF_UNEXPLAINED_MISSING_SOURCE_ID");
            if(next!=null && !prior.sourceObservationId().equals(next.sourceObservationId())) {
                require(affected.contains(month(prior.eventTimeUtc())) && affected.contains(month(next.eventTimeUtc())),"ASOF_MOVE_REQUIRES_EXPLICIT_CLOSURE");
                require(next.sourceUpdatedAtUtc()!=null && (prior.sourceUpdatedAtUtc()==null
                        || next.sourceUpdatedAtUtc().isAfter(prior.sourceUpdatedAtUtc())),"ASOF_STALE_OR_AMBIGUOUS_REVISION");
            }
        }
        var pins=new TreeMap<>(priorPins); pins.putAll(incomingPins);
        // Stable verified Bronze lineage for repeated IDs; new revisions retain history.
        var unique=new TreeMap<String,SilverObservation>(); old.observations().forEach(o->unique.putIfAbsent(o.sourceObservationId(),o));
        for(var row:incoming.observations()) {
            var prior=unique.putIfAbsent(row.sourceObservationId(),row);
            require(prior==null || prior.rawRecordHash().equals(row.rawRecordHash()),"ASOF_REVISION_ID_CONTENT_CONFLICT");
        }
        require(unique.size()<=MAX_ROWS,"ASOF_STATE_ROW_BUDGET");
        var lineage=new TreeMap<String,String>();
        for(var pin:pins.values()) { var input=capture.read(pin); lineage.put(text(input.manifest(),"manifest_id"),text(input.manifest(),"run_id")); }
        Instant processed=utc(selected,"observation_cutoff_utc");
        var integrated=new SilverMultiSourceIntegrationRunner().run(op,new ArrayList<>(unique.values()),List.of(),
                SilverLinkConfig.defaultConfig(),null,false,processed,lineage);
        require(integrated.isPublishable() && integrated.qualityResult().rejectedCount()==0 && integrated.sourceLinks().isEmpty(),"ASOF_SILVER_QUALITY_BLOCKED");
        var frame=spark.createDataFrame(integrated.allObservations().stream().map(SilverObservation::toRow).toList(),SilverSchemas.OBSERVATION_SCHEMA);
        require(frame.count()==unique.size() && frame.select("source_observation_id").distinct().count()==unique.size(),"ASOF_OBSERVATION_GRAIN_CHANGED");
        String selectionSha=sha(JSON.writeValueAsBytes(canonical(selected)));
        for(var s:scopes(request)) {
            var proof=coverage.putObject(s.month()); proof.put("event_start_utc",s.start().toString()); proof.put("event_end_utc_exclusive",s.end().toString());
            proof.put("operation_id",op); proof.put("selection_sha256",selectionSha);
            proof.put("observation_cutoff_utc",processed.toString());
        }
        require(coverage.size()<=480,"ASOF_REGISTRY_MONTH_BUDGET");
        // Retain exact raw evidence only for selected revisions or the latest monthly coverage.
        var needed=new HashSet<String>(); integrated.allObservations().forEach(o->needed.add(o.bronzeManifestId()));
        var retained=new TreeMap<String,JsonNode>();
        for(var pin:pins.values()) if(needed.contains(text(capture.read(pin).manifest(),"manifest_id"))) retained.put(text(pin,"manifest_uri"),pin);
        validateCoverage(coverage);
        for(var proof:coverage) {
            var req=readCaptureRequest(text(proof,"operation_id")); var cap=capture.verify(req);
            require(sha(JSON.writeValueAsBytes(canonical(cap))).equals(text(proof,"selection_sha256")),"ASOF_COVERAGE_SELECTION_CHANGED");
            for(var part:cap.path("parts")) {
                var pin=part.path("pin");
                if(utc(pin,"window_start_utc").isBefore(utc(proof,"event_end_utc_exclusive"))
                        && utc(pin,"window_end_utc").isAfter(utc(proof,"event_start_utc"))) retained.put(text(pin,"manifest_uri"),pin);
            }
        }
        checkBudget(retained.values());
        var reconciliation=JSON.createObjectNode();
        reconciliation.put("baseline_unique_revisions",old.observations().size()); reconciliation.put("capture_records",incoming.records());
        reconciliation.put("capture_unique_revisions",incoming.observations().size()); reconciliation.put("capture_repeated_ids",incoming.records()-incoming.observations().size());
        reconciliation.put("reused_revision_ids",old.observations().size()+incoming.observations().size()-unique.size());
        reconciliation.put("output_revisions",unique.size()); reconciliation.put("current",integrated.currentObservations().size());
        reconciliation.put("history",integrated.dedupResult().historyObservations().size()); reconciliation.put("memberships",integrated.canonicalMemberships().size());
        reconciliation.put("links",0); reconciliation.put("parser_rejects",0); reconciliation.put("quality_rejects",0);
        reconciliation.put("balanced",integrated.reconciliationReport().isReconciliationBalanced());
        var context=flags(); context.put("quality_passed",true); context.put("reconciliation_balanced",true);
        context.put("request_sha256",requestSha); context.set("month_coverage",coverage); context.set("reconciliation",reconciliation);
        guard.run(); immutable(root+"/identity.sha256",requestSha.getBytes(StandardCharsets.US_ASCII));
        new SilverBundlePublisher(store).publish(new SilverWriteRequest(op,integrated.allObservations(),List.of(),List.of(),
                integrated.canonicalMemberships(),processed,false),context);
        var registry=flags(); registry.put("status","ObservedAsOfReady"); registry.put("operation_id",op);
        registry.set("request",canonical(request)); registry.put("request_sha256",requestSha);
        registry.put("event_cutoff_utc",cutoff(request).toString()); registry.put("observation_cutoff_utc",processed.toString());
        registry.set("month_coverage",coverage); registry.set("active_inputs",JSON.valueToTree(retained.values()));
        registry.set("source_index",index(integrated.currentObservations()));
        registry.put("silver_bundle_sha256",sha(store.read(SilverBundlePublisher.root(op)+"/manifest.json")));
        immutable(root+"/registry.json",JSON.writeValueAsBytes(canonical(registry)));
        var receipt=flags(); receipt.put("status","ObservedAsOfReady"); receipt.put("operation_id",op); receipt.put("request_sha256",requestSha);
        receipt.put("registry_sha256",sha(store.read(root+"/registry.json"))); receipt.put("registry_uri",store.uriForKey(root+"/registry.json"));
        receipt.put("silver_bundle_uri",store.uriForKey(SilverBundlePublisher.root(op)+"/manifest.json"));
        receipt.put("silver_bundle_sha256",text(registry,"silver_bundle_sha256")); receipt.put("event_cutoff_utc",cutoff(request).toString());
        receipt.put("observation_cutoff_utc",processed.toString()); receipt.set("affected_months",JSON.valueToTree(affected)); receipt.set("reconciliation",reconciliation);
        immutable(root+"/receipt.json",JSON.writeValueAsBytes(canonical(receipt)));
        immutable(root+"/_SUCCESS",sha(store.read(root+"/receipt.json")).getBytes(StandardCharsets.US_ASCII));
        return verify(op,null);
    }
    public ObjectNode verify(String op,String expectedRegistrySha) throws Exception {
        guard.run(); String root=root(op); byte[] bytes=store.read(root+"/registry.json"), receiptBytes=store.read(root+"/receipt.json");
        require(Arrays.equals(store.read(root+"/_SUCCESS"),sha(receiptBytes).getBytes(StandardCharsets.US_ASCII)),"ASOF_SILVER_MARKER_CHANGED");
        var receipt=JSON.readTree(receiptBytes); var registry=JSON.readTree(bytes);
        require((expectedRegistrySha==null || sha(bytes).equals(expectedRegistrySha)) && sha(bytes).equals(text(receipt,"registry_sha256")),"ASOF_REGISTRY_PIN_CHANGED");
        var request=registry.path("request"); CurrentMonthContract.validate(request,clock);
        require(op.equals(operation(request)) && digest(request).equals(text(registry,"request_sha256"))
                && digest(request).equals(text(receipt,"request_sha256")),"ASOF_REGISTRY_IDENTITY_CHANGED");
        for(var it=flags().fields();it.hasNext();) { var f=it.next(); require(f.getValue().equals(receipt.path(f.getKey())) && f.getValue().equals(registry.path(f.getKey())),"ASOF_POLICY_PROMOTION_BLOCKED"); }
        var selected=capture.verify(request);
        require(utc(selected,"observation_cutoff_utc").equals(utc(registry,"observation_cutoff_utc")),"ASOF_REGISTRY_CLOCK_CHANGED");
        var pins=new ArrayList<JsonNode>(); registry.path("active_inputs").forEach(pins::add);
        var parsed=parse(pins); var dedup=new SourceDedupTransformer().deduplicate(parsed.observations());
        require(index(dedup.currentObservations()).equals(registry.path("source_index")),"ASOF_STATE_INDEX_CHANGED");
        var r=receipt.path("reconciliation");
        for(String field:List.of("baseline_unique_revisions","capture_records","capture_unique_revisions","capture_repeated_ids","reused_revision_ids"))
            require(r.path(field).isIntegralNumber() && r.path(field).canConvertToInt() && r.path(field).asInt()>=0
                    && r.path(field).asInt()<=MAX_ROWS,"ASOF_RECONCILIATION_CHANGED");
        for(var entry:Map.of("output_revisions",parsed.observations().size(),"current",dedup.currentObservations().size(),
                "history",dedup.historyObservations().size(),"memberships",dedup.currentObservations().size(),
                "links",0,"parser_rejects",0,"quality_rejects",0).entrySet())
            require(r.path(entry.getKey()).isIntegralNumber() && r.path(entry.getKey()).asLong(-1)==entry.getValue(),"ASOF_RECONCILIATION_CHANGED");
        require(r.path("balanced").isBoolean() && r.path("balanced").asBoolean(),"ASOF_RECONCILIATION_CHANGED");
        require(r.path("capture_records").asLong()==r.path("capture_unique_revisions").asLong()+r.path("capture_repeated_ids").asLong()
                && r.path("baseline_unique_revisions").asLong()+r.path("capture_unique_revisions").asLong()-r.path("reused_revision_ids").asLong()==r.path("output_revisions").asLong(),"ASOF_RECONCILIATION_CHANGED");
        validateCoverage(registry.path("month_coverage"));
        var bundle=new SilverBundlePublisher(store).verify(op);
        require(sha(store.read(SilverBundlePublisher.root(op)+"/manifest.json")).equals(text(registry,"silver_bundle_sha256"))
                && text(registry,"silver_bundle_sha256").equals(text(receipt,"silver_bundle_sha256"))
                && registry.path("month_coverage").equals(bundle.path("context").path("month_coverage"))
                && receipt.path("reconciliation").equals(bundle.path("context").path("reconciliation")),"ASOF_SILVER_BUNDLE_CHANGED");
        return (ObjectNode)receipt;
    }
    private record Parsed(List<SilverObservation> observations,int records) { }
    private Parsed parse(Collection<JsonNode> pins) throws IOException {
        checkBudget(pins); var rows=new TreeMap<String,SilverObservation>(); int records=0;
        for(var pin:pins) {
            var input=capture.read(pin); var m=input.manifest();
            var parsed=new UsgsGeoJsonParser().parse(input.raw(),new UsgsParseContext(text(m,"manifest_id"),text(m,"raw_object_uri"),
                    text(m,"sha256"),text(m,"run_id"),utc(m,"retrieved_at_utc")));
            require(parsed.rejects().isEmpty(),"ASOF_PARSER_REJECTS_BLOCKED"); records+=parsed.observations().size();
            for(var o:parsed.observations()) {
                require(!o.eventTimeUtc().isBefore(utc(pin,"window_start_utc")) && o.eventTimeUtc().isBefore(utc(pin,"window_end_utc")),"ASOF_EVENT_OUTSIDE_CAPTURE");
                var prior=rows.putIfAbsent(o.sourceObservationId(),o);
                require(prior==null || prior.rawRecordHash().equals(o.rawRecordHash()),"ASOF_REVISION_ID_CONTENT_CONFLICT");
                require(rows.size()<=MAX_ROWS,"ASOF_STATE_ROW_BUDGET");
            }
        }
        return new Parsed(new ArrayList<>(rows.values()),records);
    }
    private void checkBudget(Collection<JsonNode> pins) throws IOException {
        require(pins.size()<=MAX_PARTS,"ASOF_ACTIVE_INPUT_BUDGET"); long bytes=0; var seen=new HashSet<String>();
        for(var pin:pins) { require(seen.add(text(pin,"manifest_uri")),"ASOF_DUPLICATE_ACTIVE_PIN"); bytes+=capture.read(pin).raw().length; require(bytes<=MAX_BYTES,"ASOF_STATE_BYTE_BUDGET"); }
    }
    private JsonNode readCaptureRequest(String op) throws IOException {
        // Capture owns the store boundary; obtain an exact stored request without listing/latest.
        return capture.request(op);
    }
    private void validateCoverage(JsonNode coverage) throws IOException {
        require(coverage.isObject() && !coverage.isEmpty() && coverage.size()<=480,"ASOF_MONTH_COVERAGE_REQUIRED");
        var checked=new HashMap<String,JsonNode>();
        for(var it=coverage.fields();it.hasNext();) {
            var entry=it.next(); var proof=entry.getValue();
            fields(proof,Set.of("event_start_utc","event_end_utc_exclusive","operation_id","selection_sha256","observation_cutoff_utc"));
            String op=text(proof,"operation_id"); JsonNode cap=checked.get(op);
            if(cap==null) { cap=capture.verify(readCaptureRequest(op)); checked.put(op,cap); }
            require(sha(JSON.writeValueAsBytes(canonical(cap))).equals(text(proof,"selection_sha256"))
                    && utc(cap,"observation_cutoff_utc").equals(utc(proof,"observation_cutoff_utc")),"ASOF_COVERAGE_SELECTION_CHANGED");
            var matches=scopes(cap.path("request")).stream().filter(s->s.month().equals(entry.getKey())).toList();
            require(matches.size()==1 && matches.get(0).start().equals(utc(proof,"event_start_utc"))
                    && matches.get(0).end().equals(utc(proof,"event_end_utc_exclusive")),"ASOF_MONTH_COVERAGE_CHANGED");
        }
    }
    private JsonNode index(List<SilverObservation> rows) {
        var index=JSON.createObjectNode();
        rows.stream().sorted(Comparator.comparing(SilverObservation::sourceRecordKey)).forEach(o->{
            var item=index.putObject(o.sourceRecordKey()); item.put("observation_id",o.sourceObservationId()); item.put("month",month(o.eventTimeUtc()));
        }); return index;
    }
    private void immutable(String key,byte[] bytes) throws IOException {
        guard.run(); if(store.exists(key)) require(Arrays.equals(store.read(key),bytes),"ASOF_IMMUTABLE_STATE_CONFLICT");
        else store.put(key,bytes,"application/json"); require(Arrays.equals(store.read(key),bytes),"ASOF_STATE_READBACK_FAILED");
    }
    /** Fixed namespace isolation; legacy readers cannot discover it as bundles/<operation>. */
    public static final class OperationalStore implements SilverObjectStore {
        private final SilverObjectStore delegate;
        public OperationalStore(SilverObjectStore delegate) { this.delegate=Objects.requireNonNull(delegate); }
        private String key(String key) {
            if(key==null || !key.matches("[A-Za-z0-9_./=~-]*") || key.startsWith("/") || key.contains("..") || key.contains("//")) throw new IllegalArgumentException("ASOF_SAFE_KEY_REQUIRED");
            return "current-month/"+key;
        }
        public void put(String k,byte[] v,String type) throws IOException { delegate.put(key(k),v,type); }
        public byte[] read(String k) throws IOException { return delegate.read(key(k)); }
        public boolean exists(String k) throws IOException { return delegate.exists(key(k)); }
        public List<String> list(String prefix) throws IOException { return delegate.list(key(prefix)).stream().map(k->k.substring("current-month/".length())).toList(); }
        public String uriForKey(String k) { return delegate.uriForKey(key(k)); }
        public void delete(String k) throws IOException { throw new IOException("ASOF_DELETE_FORBIDDEN"); }
        public void deletePrefix(String k) throws IOException { throw new IOException("ASOF_DELETE_FORBIDDEN"); }
        public void move(String a,String b) throws IOException { throw new IOException("ASOF_MOVE_FORBIDDEN"); }
    }
}
