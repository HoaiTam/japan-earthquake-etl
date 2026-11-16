package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.*;
import java.util.*;

/** Part 1 only: versioned prepared inputs/coverage, never a Published or SilverReady registry. */
public final class PortableImportContract {
    public static final String VERSION = "dat03-portable-import-v1";
    public static final String COVERAGE_VERSION = "dat03-portable-source-coverage-v1";
    public static final String PREPARED_VERSION = "dat03-prepared-catalog-v1";
    static final ObjectMapper JSON = new ObjectMapper();
    private PortableImportContract() { }
    static void require(boolean ok, String reason) throws IOException { if (!ok) throw new IOException(reason); }
    public static List<String> months(String start, String endExclusive) {
        YearMonth first = YearMonth.parse(start), end = YearMonth.parse(endExclusive);
        if (!first.isBefore(end) || first.until(end, java.time.temporal.ChronoUnit.MONTHS) > 480)
            throw new IllegalArgumentException("BOUNDED_MONTH_RANGE_REQUIRED");
        var result = new ArrayList<String>(); for (var m = first; m.isBefore(end); m = m.plusMonths(1)) result.add(m.toString());
        return List.copyOf(result);
    }
    public static final List<String> ALL_MONTHS = months("2000-01", "2024-01");
    public static final List<String> BASELINE_MONTHS = months("2000-01", "2013-08");

    public static void validate(JsonNode r) throws Exception {
        require(VERSION.equals(r.path("version").asText()) && "PREPARE_ONLY".equals(r.path("mode").asText())
                && r.path("run_id").asText().matches("dat03-import-[a-f0-9]{32}")
                && r.path("code_sha256").asText().matches("[a-f0-9]{64}"), "EXACT_PREPARATION_REQUEST_REQUIRED");
        LocalDate.parse(r.path("processing_date").asText());
        Instant cutoff = Instant.parse(r.path("source_cutoff_utc").asText()), processed = Instant.parse(r.path("processed_at_utc").asText());
        require(!cutoff.isAfter(processed) && processed.atZone(ZoneOffset.UTC).toLocalDate().toString().equals(r.path("processing_date").asText()), "PROCESSING_BEFORE_SOURCE_CUTOFF_OR_WRONG_UTC_DATE");
        var context = r.path("run_context"); var manifests = new ArrayList<String>();
        require(context.path("input_manifest_uris").isArray() && context.path("is_backfill").isBoolean() && context.path("is_backfill").asBoolean(), "BACKFILL_RUN_CONTEXT_REQUIRED");
        for (var uri : context.path("input_manifest_uris")) { require(uri.isTextual(), "INPUT_URI_STRING_REQUIRED"); manifests.add(uri.asText()); }
        new GoldRunContext(context.path("run_id").asText(), Instant.parse(context.path("window_start_utc").asText()),
                Instant.parse(context.path("window_end_utc").asText()), LocalDate.parse(context.path("processing_date").asText()), true,
                context.path("config_version").asText(), manifests);
        require(context.path("run_id").equals(r.path("run_id")) && context.path("processing_date").equals(r.path("processing_date"))
                && "2000-01-01T00:00:00Z".equals(context.path("window_start_utc").asText()) && "2024-01-01T00:00:00Z".equals(context.path("window_end_utc").asText())
                && VERSION.equals(context.path("config_version").asText()), "EXACT_RESEARCH_RUN_CONTEXT_REQUIRED");
        for (String key : List.of("baseline_acceptance", "bronze_plan")) {
            var ref = r.path(key);
            require(ref.path("relative_path").asText().matches("inputs/[a-z0-9-]+\\.json")
                    && ref.path("sha256").asText().matches("[a-f0-9]{64}"), "PINNED_INPUT_REFERENCE_REQUIRED");
        }
        var baseline = r.path("baseline");
        require(baseline.path("namespace").asText().matches("iceberg\\.dat03_catalog_[a-f0-9]{32}")
                && baseline.path("gold_run_id").asText().matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
                && baseline.path("bundle_sha256").asText().matches("[a-f0-9]{64}")
                && baseline.path("acceptance_file_sha256").asText().matches("[a-f0-9]{64}")
                && baseline.path("completed_months").isIntegralNumber() && baseline.path("completed_months").canConvertToInt() && baseline.path("completed_months").asInt(-1) == 163
                && baseline.path("tables").isObject(), "PINNED_163_MONTH_BASELINE_REQUIRED");
        require(baseline.path("tables").size() == 6, "SIX_BASELINE_PINS_REQUIRED");
        for (String name : GoldIcebergWriter.TABLES) {
            var pin = baseline.path("tables").path(name);
            require(pin.path("snapshot_id").isIntegralNumber() && pin.path("snapshot_id").canConvertToLong() && pin.path("snapshot_id").asLong() > 0
                    && pin.path("count").isIntegralNumber() && pin.path("count").canConvertToLong() && pin.path("count").asLong() >= 0,
                    "EXACT_BASELINE_INT64_REQUIRED");
        }
        require(r.path("worker_batches").isArray() && r.path("worker_batches").size() > 0 && r.path("worker_batches").size() <= 32,
                "BOUNDED_WORKER_BATCHES_REQUIRED");
        var all = new ArrayList<>(BASELINE_MONTHS); var runs = new HashSet<String>();
        for (JsonNode batch : r.path("worker_batches")) {
            require(batch.path("plan_sha256").asText().matches("[a-f0-9]{64}")
                    && batch.path("plan_file").asText().matches("inputs/worker-[0-9]{2}\\.json")
                    && batch.path("original_plan_sha256").asText().matches("[a-f0-9]{64}")
                    && batch.path("months").isArray() && batch.path("months").size() > 0 && batch.path("months").size() <= 12,
                    "PINNED_TWELVE_MONTH_BATCH_REQUIRED");
            String owner = batch.path("owner").asText(); var scope = new TreeSet<String>();
            for (var month : batch.path("months")) {
                require(month.isTextual() && scope.add(month.asText()), "DISTINCT_BATCH_MONTH_REQUIRED"); YearMonth.parse(month.asText());
                require(owner.equals("ThanhTris") ? months("2013-08", "2018-10").contains(month.asText())
                        : owner.equals("rosy179") && months("2018-10", "2024-01").contains(month.asText()), "WORKER_SPLIT_SCOPE_MISMATCH");
                all.add(month.asText());
            }
            var expected = months(scope.first(), YearMonth.parse(scope.last()).plusMonths(1).toString());
            require(new ArrayList<>(scope).equals(expected) && JSON.valueToTree(expected).equals(batch.path("months")), "BATCH_MONTH_GAP_OR_ORDER");
            for (String key : List.of("observation_count", "canonical_count", "source_link_count"))
                require(batch.path(key).isIntegralNumber() && batch.path(key).canConvertToLong()
                        && batch.path(key).asLong() >= 0 && batch.path(key).asLong() <= 1_000_000, "TYPED_BOUNDED_BATCH_COUNTS_REQUIRED");
            require(batch.path("canonical_count").asLong() <= batch.path("observation_count").asLong(), "BATCH_CANONICAL_COUNT_UNBALANCED");
            require(batch.path("run_ids").isArray() && batch.path("run_ids").size() > 0 && batch.path("run_ids").size() <= 4, "BOUNDED_DISTINCT_RUNS_REQUIRED");
            for (var run : batch.path("run_ids")) require(run.isTextual() && run.asText().matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
                    && runs.add(run.asText()), "DUPLICATE_SOURCE_RUN");
        }
        require(all.size() == 288 && new HashSet<>(all).size() == 288 && all.equals(ALL_MONTHS),
                "EXACT_288_MONTHS_NO_GAP_OR_OVERLAP_REQUIRED");
        for (String flag : List.of("published", "research_ready", "official_export_ready", "full_capacity_accepted", "scientific_approval"))
            require(!r.has(flag), "CALLER_READINESS_FLAGS_FORBIDDEN");
    }

    /** Called only after fresh Java Bronze readback; the return value is not legacy FullMonthReady. */
    public static ObjectNode coverage(JsonNode verifiedPhysicalPins, String provenanceSha) throws Exception {
        require(provenanceSha.matches("[a-f0-9]{64}") && verifiedPhysicalPins.isArray() && verifiedPhysicalPins.size() == 37,
                "VERIFIED_37_SOURCE_SET_REQUIRED");
        var years = new TreeMap<Integer, String>(); var usgs = new ArrayList<JsonNode>(); var uris = new HashSet<String>();
        for (var pin : verifiedPhysicalPins) {
            String uri = pin.path("manifest_uri").asText();
            require(uri.matches("s3://[a-z0-9.-]+/bronze/[A-Za-z0-9=._~/-]+/manifest.json") && !uri.contains("..") && uris.add(uri)
                    && pin.path("manifest_sha256").asText().matches("[a-f0-9]{64}"), "DISTINCT_PINNED_SOURCE_REQUIRED");
            if ("JMA_BULLETIN".equals(pin.path("source_system").asText())) {
                require(pin.path("year").isIntegralNumber() && pin.path("year").canConvertToInt()
                        && "full-year".equals(pin.path("segment").asText()) && years.put(pin.path("year").asInt(), uri) == null,
                        "EXACT_JMA_YEAR_REQUIRED");
            } else { require("USGS".equals(pin.path("source_system").asText()), "UNKNOWN_SOURCE"); usgs.add(pin); }
        }
        require(years.keySet().equals(new TreeSet<>(java.util.stream.IntStream.rangeClosed(2000, 2024).boxed().toList())) && usgs.size() == 12,
                "JMA_2000_2024_AND_USGS_12_REQUIRED");
        var result = JSON.createObjectNode().put("version", COVERAGE_VERSION).put("status", "PREPARED_SOURCE_COVERAGE")
                .put("provenance_receipt_sha256", provenanceSha).put("reproduction_months", 225).put("extension_months", 63);
        var rows = result.putArray("months");
        for (String value : ALL_MONTHS) {
            YearMonth m = YearMonth.parse(value); var row = rows.addObject().put("month", value)
                    .put("split", value.compareTo("2018-10") < 0 ? "REPRODUCTION" : "EXTENSION");
            var jma = row.putArray("jma_physical_manifest_uris"); jma.add(years.get(m.getYear()));
            if (m.getMonthValue() == 12) jma.add(years.get(m.getYear() + 1));
            var secondary = row.putArray("usgs_physical_manifest_uris");
            String required = value.equals("2022-12") ? "2023-01" : value.startsWith("2023-") ? value : null;
            if (required != null) {
                YearMonth target = YearMonth.parse(required); Instant start = target.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
                Instant end = target.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(); int matched = 0;
                for (var pin : usgs) {
                    Instant a = Instant.parse(pin.path("window_start_utc").asText()), b = Instant.parse(pin.path("window_end_utc").asText());
                    if (!a.isAfter(start) && !b.isBefore(end) && !a.isBefore(start.minusSeconds(16)) && !b.isAfter(end.plusSeconds(16))) {
                        secondary.add(pin.path("manifest_uri").asText()); matched++;
                    }
                }
                require(matched == 1, "EXACT_USGS_MONTH_AND_HALO_REQUIRED");
            }
        }
        return result.put("month_count", 288).put("gold_published", false).put("official_export_ready", false).put("research_ready", false);
    }
}
