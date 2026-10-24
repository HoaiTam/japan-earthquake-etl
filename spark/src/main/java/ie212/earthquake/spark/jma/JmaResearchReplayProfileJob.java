package ie212.earthquake.spark.jma;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.orchestration.FullMonthBronzeReader;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import ie212.earthquake.spark.silver.SourceKeyGenerator;
import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Local real-byte reader probe, NOT a BronzeReady/source coverage/resolver/Gold acceptance. */
public final class JmaResearchReplayProfileJob {
    private JmaResearchReplayProfileJob() { }
    public static ObjectNode profile(Path archive,String member,String expectedSha,int year,Set<String> selectedMonths) throws Exception {
        require(expectedSha.matches("[a-f0-9]{64}") && year>=1984 && year<=2024 && !selectedMonths.isEmpty()
                && selectedMonths.size()<=24 && selectedMonths.stream().allMatch(m->m.matches("[12][0-9]{3}-(0[1-9]|1[0-2])")),"REPLAY_PROFILE_ARGUMENTS_REQUIRED");
        require(Files.isRegularFile(archive) && Files.size(archive)<=32L*1024*1024,"BOUNDED_RAW_INPUT_REQUIRED");
        byte[] raw=Files.readAllBytes(archive); require(SourceKeyGenerator.sha256(raw).equals(expectedSha),"PROFILE_ARCHIVE_PIN_MISMATCH");
        var checked=new JmaArchiveValidator().validate(raw,member); require(checked.valid(),"JMA_FULL_ZIP_REQUIRED");
        // A private in-memory parse descriptor deliberately has NO BronzeReady/validation flags,
        // request/HTTP metadata or manifest URI. It can never be used as a source pin.
        var metadata=JSON.createObjectNode().put("source_system","JMA_BULLETIN").put("raw_object_uri",archive.toAbsolutePath().toUri().toString())
                .put("sha256",expectedSha).put("run_id","dat03-local-reader-probe").put("manifest_id","local-probe-"+expectedSha)
                .put("catalog_release","engineering-probe-not-source-release").put("retrieved_at_utc",Instant.EPOCH.toString())
                .put("record_count_estimate",checked.recordCount());
        metadata.putObject("provenance").put("member_name",member);
        // Parser requires an HTTP-shaped provenance string. The reserved .invalid domain
        // makes the engineering-only descriptor explicit; no network request is made.
        metadata.putObject("request").put("url","https://example.invalid/dat03-local-engineering-probe");
        Instant from=LocalDate.of(year,1,1).atStartOfDay(ZoneId.of("Asia/Tokyo")).toInstant();
        Instant to=LocalDate.of(year+1,1,1).atStartOfDay(ZoneId.of("Asia/Tokyo")).toInstant();
        var input=new FullMonthBronzeReader.Input(JSON.createObjectNode(),metadata,raw,from,to);
        // No reads or writes outside the exact local capture are permitted by this probe.
        BronzeObjectStore forbidden=new BronzeObjectStore() {
            public void putIfAbsent(String k,byte[] b,String t) { throw new UnsupportedOperationException("PROBE_STORE_FORBIDDEN"); }
            public byte[] read(String k) { throw new UnsupportedOperationException("PROBE_STORE_FORBIDDEN"); }
            public boolean exists(String k) { throw new UnsupportedOperationException("PROBE_STORE_FORBIDDEN"); }
            public String uriForKey(String k) { throw new UnsupportedOperationException("PROBE_STORE_FORBIDDEN"); }
        };
        var reader=new FullMonthBronzeReader(forbidden,List.of());
        var report=JSON.createObjectNode().put("profile_version","dat03-reader-replay-v1")
                .put("profile_kind","ENGINEERING_ONLY_NOT_RESOLVER_ACCEPTANCE").put("archive_sha256",expectedSha)
                .put("archive_bytes",raw.length).put("native_year",year).put("member",member).put("physical_records",checked.recordCount())
                .put("max_selected_revisions_per_operation",MAX_ROWS).put("research_ready",false).put("coverage_verified",false)
                .put("gold_published",false).put("full_resolver_verified",false).put("source_authenticity_verified",false);
        var operations=report.putArray("single_month_operations");
        for(String m:new TreeSet<>(selectedMonths)) {
            long started=System.nanoTime(); var result=reader.parseWithReport(List.of(input),Set.of(m));
            operations.addObject().put("month",m).put("selected_input_records",result.selectedRows()).put("unique_revisions",result.observations().size())
                    .put("elapsed_ms",(System.nanoTime()-started)/1_000_000).put("status","PASSED");
        }
        try {
            var combined=reader.parseWithReport(List.of(input),selectedMonths);
            report.put("combined_scope_status","PASSED").put("combined_unique_revisions",combined.observations().size());
        } catch(IOException error) {
            if(!"BOUNDED_MONTHLY_ROW_LIMIT".equals(error.getMessage())) throw error;
            report.put("combined_scope_status","BLOCKED_BY_UNCHANGED_ROW_GUARD").put("combined_reason",error.getMessage());
        }
        require(SourceKeyGenerator.sha256(Files.readAllBytes(archive)).equals(expectedSha),"PROFILE_ARCHIVE_CHANGED_DURING_READ");
        return report;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==6,"archive member expected_sha256 native_year comma_separated_months output_json");
        var months=new TreeSet<>(List.of(args[4].split(",",-1))); require(months.size()==args[4].split(",",-1).length,"DUPLICATE_MONTH");
        var report=profile(Path.of(args[0]),args[1],args[2],Integer.parseInt(args[3]),months);
        Files.write(Path.of(args[5]),JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report),StandardOpenOption.CREATE_NEW);
        System.out.println("event=dat03_reader_replay_profiled combined_status="+report.path("combined_scope_status").asText()+" research_ready=false coverage_verified=false");
    }
}
