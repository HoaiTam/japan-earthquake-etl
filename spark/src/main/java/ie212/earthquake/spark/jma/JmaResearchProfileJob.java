package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.JmaFixedWidthParser;
import ie212.earthquake.spark.silver.JmaParseContext;
import ie212.earthquake.spark.silver.SourceKeyGenerator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipFile;

/** Engineering profile only. Reuses the JMA parser, holds no observation catalog, writes no lake data. */
public final class JmaResearchProfileJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private JmaResearchProfileJob() { }

    public static ObjectNode profile(Path archive, String member, String expectedSha256) throws Exception {
        if (!Files.isRegularFile(archive, LinkOption.NOFOLLOW_LINKS)
                || expectedSha256 == null || !expectedSha256.matches("[a-f0-9]{64}")) {
            throw new IOException("PROFILE_EXACT_LOCAL_ARCHIVE_AND_SHA_REQUIRED");
        }
        if (Files.size(archive) > JmaArchiveValidator.DEFAULT_MAX_ARCHIVE_BYTES) {
            throw new IOException("PROFILE_ARCHIVE_SIZE_LIMIT");
        }
        byte[] bytes = Files.readAllBytes(archive);
        if (!SourceKeyGenerator.sha256(bytes).equals(expectedSha256)) {
            throw new IOException("PROFILE_ARCHIVE_SHA_MISMATCH");
        }
        var validation = new JmaArchiveValidator().validate(bytes, member);
        if (!validation.valid()) throw new IOException("PROFILE_ARCHIVE_INVALID:" + validation.reason());
        var context = new JmaParseContext("profile-local-only", archive.toUri().toString(), expectedSha256,
                "dat03-profile", "profile-not-a-catalog-release", null, null, member, Instant.EPOCH);
        var parser = new JmaFixedWidthParser();
        Map<String, Long> counts = new TreeMap<>(), months = new TreeMap<>(), reasons = new TreeMap<>();
        Instant first = null, last = null;
        int lineNumber = 0;
        long memberBytes;
        try (var zip = new ZipFile(archive.toFile());
                var reader = new BufferedReader(new InputStreamReader(zip.getInputStream(zip.getEntry(member)), StandardCharsets.ISO_8859_1))) {
            memberBytes = zip.getEntry(member).getSize();
            String line;
            while ((line = reader.readLine()) != null) {
                var parsed = parser.parseVerifiedRecord(line.getBytes(StandardCharsets.ISO_8859_1), ++lineNumber, context);
                add(counts, "physical_records");
                if (parsed.rejectedCount() != 0) {
                    add(counts, "parser_rejected");
                    parsed.rejects().forEach(r -> r.rejectReasonCodes().forEach(reason -> add(reasons, reason)));
                    continue;
                }
                add(counts, "parser_accepted");
                var row = parsed.observations().get(0);
                var time = row.eventTimeUtc();
                if (first == null || time.isBefore(first)) first = time;
                if (last == null || time.isAfter(last)) last = time;
                add(months, YearMonth.from(time.atZone(ZoneOffset.UTC)).toString());
                if (!row.qualityFlags().isEmpty()) add(counts, "parser_warning_rows");
                if (row.depthKm() == null) add(counts, "missing_depth");
                if (row.magnitude() == null) add(counts, "missing_magnitude");
                // This is before quality/dedup/canonical/Mc/window gates, NOT final research eligibility.
                if (row.isInStudyArea() && "EARTHQUAKE".equals(row.eventTypeCode())
                        && "UNIFIED".equals(row.catalogEra()) && "J".equals(row.determiningAgencyCode())
                        && row.depthKm() != null && row.depthKm() >= 0 && row.magnitude() != null) {
                    add(counts, "pre_mc_research_filter_rows");
                    if (row.depthKm() >= 50 && row.depthKm() <= 200 && row.magnitude() > 5.5) {
                        add(counts, "pre_window_mainshock_rows");
                    }
                }
            }
        }
        if (lineNumber != validation.recordCount()
                || !SourceKeyGenerator.sha256(Files.readAllBytes(archive)).equals(expectedSha256)) {
            throw new IOException("PROFILE_ARCHIVE_CHANGED_DURING_READ");
        }
        for (String name : new String[]{"physical_records", "parser_accepted", "parser_rejected",
                "parser_warning_rows", "missing_depth", "missing_magnitude",
                "pre_mc_research_filter_rows", "pre_window_mainshock_rows"}) counts.putIfAbsent(name, 0L);
        var result = JSON.createObjectNode();
        result.put("profile_version", "dat03-local-archive-v1");
        result.put("profile_kind", "ENGINEERING_ONLY_NOT_COVERAGE_PROOF");
        result.put("source_system", "JMA_BULLETIN"); result.put("member", member);
        result.put("archive_sha256", expectedSha256); result.put("archive_bytes", bytes.length);
        result.put("member_bytes", memberBytes); result.put("structural_validation", "PASSED");
        result.put("parser_version", JmaFixedWidthParser.PARSER_VERSION);
        result.put("research_ready", false); result.put("gold_published", false);
        result.put("source_authenticity_verified", false); result.put("coverage_verified", false);
        result.set("counts", JSON.valueToTree(counts)); result.set("accepted_rows_by_utc_month", JSON.valueToTree(months));
        result.set("parser_rejection_reason_counts", JSON.valueToTree(reasons));
        if (first == null) { result.putNull("first_accepted_event_utc"); result.putNull("last_accepted_event_utc"); }
        else { result.put("first_accepted_event_utc", first.toString()); result.put("last_accepted_event_utc", last.toString()); }
        result.put("counts_reconciled", counts.get("physical_records") == counts.get("parser_accepted") + counts.get("parser_rejected"));
        return result;
    }

    private static void add(Map<String, Long> counts, String key) { counts.merge(key, 1L, Long::sum); }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("archive member expected_sha256 output_json");
        var report = profile(Path.of(args[0]), args[1], args[2]);
        Files.writeString(Path.of(args[3]), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n",
                StandardOpenOption.CREATE_NEW);
        System.out.println("event=dat03_archive_profiled physical_records=" + report.path("counts").path("physical_records").asLong()
                + " research_ready=false coverage_verified=false");
    }
}
