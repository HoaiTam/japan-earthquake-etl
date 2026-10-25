package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.zip.ZipInputStream;

/** Exact immutable payload readback + computed source coverage, including explicit empty responses. */
public final class FullMonthBronzeReader {
    public record Input(JsonNode pin, JsonNode manifest, byte[] raw, Instant start, Instant end) { }
    public record Parsed(List<SilverObservation> observations, ArrayNode sources, int selectedRows) { }
    private final BronzeObjectStore store;
    private final List<JmaArchiveEntry> inventory;
    private final JmaStructureCache structureCache;
    public FullMonthBronzeReader(BronzeObjectStore store, List<JmaArchiveEntry> inventory) { this(store,inventory,null); }
    public FullMonthBronzeReader(BronzeObjectStore store,List<JmaArchiveEntry> inventory,JmaStructureCache cache) {
        this.store=store;this.inventory=List.copyOf(inventory);this.structureCache=cache;
    }
    /** Opt-in JVM-local memo of a pure structural function, never raw bytes or source coverage.
     * Every read still fetches/re-hashes the actual object and validates its manifest/inventory/cutoff.
     * No API accepts caller-supplied cache entries. A new JVM/JAR starts empty. */
    public static final class JmaStructureCache {
        private final LinkedHashMap<String,JmaArchiveValidationResult> values=new LinkedHashMap<>(16,0.75f,true);
        private long scans,hits;
        private synchronized JmaArchiveValidationResult validate(byte[] raw,String rawSha,String member) throws IOException {
            String key=rawSha+"/"+member;var prior=values.get(key);
            if(prior!=null){hits++;return prior;}
            scans++;var result=new JmaArchiveValidator().validate(raw,member);
            if(result.valid()) {
                if(values.size()==64)values.remove(values.keySet().iterator().next());
                values.put(key,result);
            }
            return result;
        }
        public synchronized long fullScans(){return scans;}
        public synchronized long hits(){return hits;}
        public synchronized int entries(){return values.size();}
    }

    /** Full validation still happens now, but a registry does not retain every archive's raw bytes. */
    public Input descriptor(JsonNode pin, Instant cutoff) throws IOException {
        var verified=read(pin,cutoff);
        return new Input(verified.pin(),verified.manifest(),null,verified.start(),verified.end());
    }
    private Input materialize(Input input) throws IOException {
        if(input.raw()!=null) return input;
        var fresh=read(input.pin(),utc(input.manifest(),"retrieved_at_utc"));
        require(fresh.manifest().equals(input.manifest()),"MANIFEST_DESCRIPTOR_CHANGED");
        return fresh;
    }
    public Set<String> changedDescriptorMonths(Input prior,Input incoming) throws IOException {
        // Keep one raw archive at a time, rather than materializing the whole registry.
        var before=recordMonths(materialize(prior));
        var after=recordMonths(materialize(incoming)); var changed=new TreeSet<String>();
        before.forEach((hash,month)->{ if(!after.containsKey(hash)) changed.add(month); });
        after.forEach((hash,month)->{ if(!before.containsKey(hash)) changed.add(month); });
        return changed;
    }
    static boolean intersects(Input input,Set<String> scope) {
        return scope.stream().anyMatch(m->input.start().isBefore(end(m).plusSeconds(HALO_SECONDS))
                && input.end().isAfter(start(m).minusSeconds(HALO_SECONDS)));
    }

    /** Whole native archive record comparison, not a guessed cross-release JMA event key. */
    public static Set<String> changedJmaMonths(Input prior,Input incoming) throws IOException {
        var before=recordMonths(prior); var after=recordMonths(incoming); var changed=new TreeSet<String>();
        before.forEach((hash,month)->{ if(!after.containsKey(hash)) changed.add(month); });
        after.forEach((hash,month)->{ if(!before.containsKey(hash)) changed.add(month); });
        return changed;
    }
    private static Map<String,String> recordMonths(Input input) throws IOException {
        var out=new HashMap<String,String>();
        try(var zip=new ZipInputStream(new ByteArrayInputStream(input.raw()))) {
            var entry=zip.getNextEntry(); require(entry!=null && entry.getName().equals(input.manifest().path("provenance").path("member_name").asText()),"JMA_MEMBER_REQUIRED");
            var lines=new BufferedReader(new InputStreamReader(zip,StandardCharsets.ISO_8859_1)); String line; long count=0;
            while((line=lines.readLine())!=null) {
                byte[] record=line.getBytes(StandardCharsets.ISO_8859_1); Instant time=JmaFixedWidthParser.originTimeOfVerifiedRecord(record);
                require(time!=null && !time.isBefore(input.start()) && time.isBefore(input.end()),"UNCLASSIFIABLE_JMA_RECORD");
                out.put(sha(record),month(time)); count++; require(count<=1_000_000,"JMA_COMPARISON_RESOURCE_LIMIT");
            }
            require(count==input.manifest().path("record_count_estimate").asLong(-1),"JMA_PHYSICAL_COUNT_MISMATCH");
        }
        return out;
    }

    public Input read(JsonNode pin, Instant cutoff) throws IOException {
        require(pin.isObject(),"TYPED_INPUT_PIN_REQUIRED");
        String declared=text(pin,"source_system");
        var fields=new HashSet<>(Set.of("source_system","manifest_uri","manifest_sha256","sha256"));
        if("USGS".equals(declared)) fields.addAll(Set.of("window_start_utc","window_end_utc"));
        else {
            require("JMA_BULLETIN".equals(declared),"TWO_SOURCE_POLICY_REQUIRED");
            fields.addAll(Set.of("year","segment","catalog_release"));
            require(pin.path("year").isIntegralNumber() && pin.path("year").canConvertToInt(),"TYPED_JMA_YEAR_REQUIRED");
            text(pin,"segment"); text(pin,"catalog_release");
        }
        var actual=new HashSet<String>(); pin.fieldNames().forEachRemaining(actual::add);
        require(actual.equals(fields),"EXACT_PIN_FIELDS_REQUIRED");
        String uri = text(pin,"manifest_uri"); byte[] bytes = store.read(key(uri,store::uriForKey));
        require(bytes.length <= 65536 && sha(bytes).equals(text(pin,"manifest_sha256")), "MANIFEST_PIN_MISMATCH");
        JsonNode m = JSON.readTree(bytes); String source = text(pin,"source_system");
        require("1.0".equals(m.path("manifest_version").asText()) && "BronzeReady".equals(m.path("bronze_status").asText())
                && source.equals(m.path("source_system").asText()), "BRONZE_READY_REQUIRED");
        for (String flag : List.of("object_write_completed", "raw_readback_verified", "checksum_verified", "source_structure_valid", "manifest_consistent"))
            require(m.path("validation").path(flag).isBoolean() && m.path("validation").path(flag).asBoolean(), "BRONZE_VALIDATION_REQUIRED");
        require(!utc(m,"retrieved_at_utc").isAfter(cutoff), "INPUT_AFTER_SOURCE_CUTOFF");
        String rawKey = key(text(m,"raw_object_uri"),store::uriForKey);
        require(rawKey.equals(text(m,"raw_object_key")), "RAW_KEY_MISMATCH");
        require(m.path("content_length_bytes").isIntegralNumber() && m.path("content_length_bytes").asLong() <= 32L*1024*1024,
                "BOUNDED_RAW_INPUT_REQUIRED");
        byte[] raw = store.read(rawKey);
        require(raw.length == m.path("content_length_bytes").asLong(-1) && sha(raw).equals(text(pin,"sha256"))
                && sha(raw).equals(text(m,"sha256")), "RAW_PIN_MISMATCH");
        var interval = m.path("data_interval"); require("[start,end)".equals(interval.path("interval_semantics").asText()), "HALF_OPEN_INTERVAL_REQUIRED");
        Instant from, to; long count;
        if ("USGS".equals(source)) {
            from = utc(interval,"window_start_utc"); to = utc(interval,"window_end_utc");
            require(from.equals(utc(pin,"window_start_utc")) && to.equals(utc(pin,"window_end_utc")), "USGS_WINDOW_PIN_MISMATCH");
            var validation = new UsgsGeoJsonValidator().validate(raw); require(validation.valid(), "USGS_STRUCTURE_REQUIRED"); count = validation.featureCount();
            JsonNode q = m.path("request").path("query");
            require(q.isObject() && q.size() == 11 && "geojson".equals(q.path("format").asText())
                    && "earthquake".equals(q.path("eventtype").asText()) && "1".equals(q.path("offset").asText())
                    && "time-asc".equals(q.path("orderby").asText()), "UNFILTERED_FIRST_PAGE_REQUIRED");
            String[] bounds = {"minlatitude","maxlatitude","minlongitude","maxlongitude"}; double[] expected = {20,50,120,155};
            for (int i=0;i<bounds.length;i++) require(Double.parseDouble(q.path(bounds[i]).asText()) == expected[i], "SOURCE_POLICY_ROI_MISMATCH");
            int limit = Integer.parseInt(q.path("limit").asText()); require(limit >= 1 && limit <= 20000 && count < limit, "USGS_TRUNCATED_OR_SATURATED_RESPONSE");
            require(Instant.parse(q.path("starttime").asText()).equals(from)
                    && Instant.parse(q.path("endtime").asText()).equals(to.minusMillis(1)), "QUERY_INTERVAL_MISMATCH");
        } else {
            require("JMA_BULLETIN".equals(source), "TWO_SOURCE_POLICY_REQUIRED");
            var matches = inventory.stream().filter(e -> e.year() == pin.path("year").asInt()
                    && e.segment().equals(pin.path("segment").asText())).toList();
            require(matches.size()==1, "PINNED_JMA_INVENTORY_REQUIRED"); var e = matches.get(0);
            require(e.nativeStartJst().equals(interval.path("native_start").asText()) && e.nativeEndJst().equals(interval.path("native_end").asText())
                    && "Asia/Tokyo".equals(interval.path("native_timezone").asText())
                    && e.memberName().equals(m.path("provenance").path("member_name").asText())
                    && e.sourceUrl().toString().equals(m.path("request").path("url").asText())
                    && pin.path("catalog_release").asText().equals(m.path("catalog_release").asText())
                    && pin.path("year").asInt() == interval.path("year").asInt()
                    && pin.path("segment").asText().equals(interval.path("segment").asText()), "JMA_INVENTORY_PIN_MISMATCH");
            var validated = structureCache==null?new JmaArchiveValidator().validate(raw,e.memberName()):structureCache.validate(raw,text(pin,"sha256"),e.memberName());
            require(validated.valid(), "JMA_FULL_ZIP_REQUIRED"); count=validated.recordCount();
            from = OffsetDateTime.parse(e.nativeStartJst()).toInstant(); to = OffsetDateTime.parse(e.nativeEndJst()).toInstant();
        }
        require(from.isBefore(to) && !to.isAfter(cutoff) && !to.isAfter(utc(m,"retrieved_at_utc")) && m.path("record_count_estimate").isIntegralNumber()
                && count == m.path("record_count_estimate").asLong(-1), "SOURCE_COUNT_OR_INTERVAL_MISMATCH");
        return new Input(pin.deepCopy(),m,raw,from,to);
    }

    public List<SilverObservation> parse(List<Input> inputs, Set<String> scope) throws IOException {
        return parseWithReport(inputs,scope).observations();
    }
    public Parsed parseWithReport(List<Input> inputs, Set<String> scope) throws IOException {
        // Collapsing identical observation IDs across overlapping responses prevents duplicate physical grain.
        var observations = new TreeMap<String,SilverObservation>();
        var sources=JSON.createArrayNode(); int selectedRows=0;
        var parser = new JmaFixedWidthParser();
        for (Input descriptor : inputs) {
            // Descriptor pins were fully verified, including CRC/count, even when not selected.
            boolean outside=descriptor.raw()==null && "JMA_BULLETIN".equals(descriptor.pin().path("source_system").asText()) && !intersects(descriptor,scope);
            Input input=outside?descriptor:materialize(descriptor);
            int selected=0, uniqueBefore=observations.size();
            JsonNode m=input.manifest(); String uri=text(m,"raw_object_uri"), hash=text(m,"sha256"), ingest=text(m,"run_id"), id=text(m,"manifest_id");
            Instant processed=utc(m,"retrieved_at_utc");
            if (outside) {
                // No observations from an unrelated archive can affect this temporal graph.
            } else if ("USGS".equals(m.path("source_system").asText())) {
                var result = new UsgsGeoJsonParser().parse(input.raw(),new UsgsParseContext(id,uri,hash,ingest,processed));
                require(result.rejects().isEmpty(), "USGS_PARSER_REJECTS_BLOCKED");
                selected=result.observations().size();
                for (var o : result.observations()) { require(!o.eventTimeUtc().isBefore(input.start()) && o.eventTimeUtc().isBefore(input.end()), "USGS_EVENT_OUTSIDE_QUERY"); observations.putIfAbsent(o.sourceObservationId(),o); }
            } else {
                String member=m.path("provenance").path("member_name").asText();
                var context=new JmaParseContext(id,uri,hash,ingest,m.path("catalog_release").asText(),null,
                        m.path("request").path("url").asText(),member,processed);
                try (var zip=new ZipInputStream(new ByteArrayInputStream(input.raw()))) {
                    var entry=zip.getNextEntry(); require(entry!=null && member.equals(entry.getName()), "JMA_MEMBER_REQUIRED");
                    var lines=new BufferedReader(new InputStreamReader(zip,StandardCharsets.ISO_8859_1)); int physical=0; String line;
                    while ((line=lines.readLine())!=null) {
                        physical++; byte[] record=line.getBytes(StandardCharsets.ISO_8859_1);
                        Instant time=JmaFixedWidthParser.originTimeOfVerifiedRecord(record);
                        require(time!=null && !time.isBefore(input.start()) && time.isBefore(input.end()), "UNCLASSIFIABLE_JMA_RECORD");
                        boolean included=false;
                        for (String month:scope) if (!time.isBefore(start(month).minusSeconds(HALO_SECONDS)) && time.isBefore(end(month).plusSeconds(HALO_SECONDS))) { included=true; break; }
                        if (!included) continue;
                        var parsed=parser.parseVerifiedRecord(record,physical,context); require(parsed.rejects().isEmpty(), "JMA_PARSER_REJECTS_BLOCKED");
                        selected+=parsed.observations().size();
                        for(var o:parsed.observations()) observations.putIfAbsent(o.sourceObservationId(),o);
                        require(observations.size()<=MAX_ROWS, "BOUNDED_MONTHLY_ROW_LIMIT");
                    }
                    require(physical == m.path("record_count_estimate").asLong(), "JMA_PHYSICAL_COUNT_MISMATCH");
                }
            }
            selectedRows+=selected; var row=sources.addObject();
            row.put("manifest_uri",input.pin().path("manifest_uri").asText()); row.put("source_system",m.path("source_system").asText());
            row.put("full_bronze_records",m.path("record_count_estimate").asLong()); row.put("selected_records",selected);
            row.put("outside_temporal_selection",m.path("record_count_estimate").asLong()-selected);
            row.put("parser_rejects",0); row.put("unique_revisions_added",observations.size()-uniqueBefore);
            row.put("repeated_observation_ids",selected-(observations.size()-uniqueBefore));
            require(observations.size()<=MAX_ROWS, "BOUNDED_MONTHLY_ROW_LIMIT");
        }
        return new Parsed(new ArrayList<>(observations.values()),sources,selectedRows);
    }
}
