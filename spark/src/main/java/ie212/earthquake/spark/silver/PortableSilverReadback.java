package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import java.util.zip.*;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.schema.MessageType;

/** Read-only portable Silver page/schema/count/relationship gate. No Spark, S3, or Gold writes. */
public final class PortableSilverReadback {
    private static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final String PREFIX = "s3://japan-earthquake/";
    private static final Map<String, MessageType> SCHEMAS = Map.of(
            "source_observation", SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA,
            "reject_record", SilverParquetSerializer.REJECT_PARQUET_SCHEMA,
            "source_link", SilverParquetSerializer.SOURCE_LINK_PARQUET_SCHEMA,
            "canonical_membership", SilverParquetSerializer.CANONICAL_MEMBERSHIP_PARQUET_SCHEMA);
    private PortableSilverReadback() { }

    private static void require(boolean ok, String reason) throws IOException {
        if (!ok) throw new IOException(reason);
    }
    private static Path input(String name) throws IOException {
        Path p = Path.of(name).toAbsolutePath().normalize();
        for (Path q = p; q != null; q = q.getParent()) require(!Files.isSymbolicLink(q), "SYMLINK_INPUT");
        require(Files.isRegularFile(p), "REGULAR_INPUT_REQUIRED");
        return p;
    }
    private static String sha(byte[] data) { return SilverBundlePublisher.sha(data); }
    private static String fileSha(Path p) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var stream = Files.newInputStream(p)) {
            byte[] buffer = new byte[1024 * 1024]; int read;
            while ((read = stream.read(buffer)) != -1) digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static String safeMember(String name) throws IOException {
        require(!name.isEmpty() && !name.startsWith("/") && !name.contains("\\") && !name.contains(":") && !name.contains("\0"), "UNSAFE_ZIP_MEMBER");
        for (String part : name.split("/", -1)) require(!part.isEmpty() && !part.equals(".") && !part.equals(".."), "UNSAFE_ZIP_MEMBER");
        return name;
    }
    private static byte[] original(JsonNode container, String name) throws IOException {
        try {
            byte[] raw = Base64.getDecoder().decode(container.path(name).asText());
            require(raw.length > 0 && raw.length <= 1024 * 1024, "METADATA_BYTE_LIMIT");
            return raw;
        } catch (IllegalArgumentException e) { throw new IOException("INVALID_BASE64_METADATA", e); }
    }

    public static ObjectNode verify(Path planPath, String expectedSha) throws Exception {
        Path planFile = input(planPath.toString());
        require(Files.size(planFile) <= 4 * 1024 * 1024 && fileSha(planFile).equals(expectedSha), "PLAN_PIN_MISMATCH");
        JsonNode plan = JSON.readTree(Files.readAllBytes(planFile));
        require("dat03-portable-silver-readback-v1".equals(plan.path("version").asText())
                && "READ_ONLY_NO_OBJECT_STORE_OR_GOLD_WRITES".equals(plan.path("mode").asText()), "READ_ONLY_PLAN_REQUIRED");
        require(plan.path("groups").isArray() && plan.path("groups").size() > 0 && plan.path("groups").size() <= 4, "ONE_TO_FOUR_GROUPS_REQUIRED");
        var months = new TreeSet<String>();
        for (JsonNode month : plan.path("affected_months")) {
            require(month.isTextual(), "MONTH_STRING_REQUIRED"); YearMonth.parse(month.asText());
            require(months.add(month.asText()), "DUPLICATE_MONTH");
        }
        require(!months.isEmpty() && months.size() <= 12, "MAX_TWELVE_MONTHS");
        require(plan.path("archives").isArray() && plan.path("archives").size() > 0 && plan.path("archives").size() <= 8, "ARCHIVE_COUNT_LIMIT");
        var archives = new HashMap<String, ZipFile>();
        long deadline = System.nanoTime() + 1200_000_000_000L;
        try {
            for (JsonNode descriptor : plan.path("archives")) {
                String expected = descriptor.path("sha256").asText();
                Path p = input(descriptor.path("path").asText());
                require(Files.size(p) <= 1024L * 1024 * 1024 && fileSha(p).equals(expected), "ARCHIVE_PIN_MISMATCH");
                require(!archives.containsKey(expected), "DUPLICATE_ARCHIVE");
                var zip = new ZipFile(p.toFile()); archives.put(expected, zip);
                var names = new HashSet<String>(); int count = 0;
                for (var entries = zip.entries(); entries.hasMoreElements();) {
                    ZipEntry entry = entries.nextElement();
                    require(++count <= 20000 && names.add(entry.getName()), "ZIP_ENTRY_SET_INVALID");
                }
            }
            var result = JSON.createObjectNode(); result.put("version", "dat03-portable-silver-readback-receipt-v1");
            result.put("plan_sha256", expectedSha); result.put("owner", plan.path("owner").asText());
            var reports = result.putArray("groups"); var allMonths = new TreeSet<String>(); var runs = new HashSet<String>();
            long bytesChecked = 0, rowsDecoded = 0;
            for (JsonNode group : plan.path("groups")) {
                require(System.nanoTime() < deadline, "READBACK_TIME_BUDGET_EXCEEDED");
                String run = group.path("run_id").asText();
                require(run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") && runs.add(run), "DISTINCT_SAFE_RUN_REQUIRED");
                byte[] bundleBytes = original(group, "bundle_base64");
                require(sha(bundleBytes).equals(group.path("bundle_sha256").asText()), "BUNDLE_PIN_MISMATCH");
                JsonNode bundle = JSON.readTree(bundleBytes); JsonNode context = bundle.path("context");
                require(run.equals(bundle.path("run_id").asText()) && "slv-09-bundle-v1".equals(bundle.path("bundle_version").asText())
                        && "SilverReady".equals(bundle.path("silver_status").asText()), "SOURCE_BUNDLE_CONTEXT_MISMATCH");
                require(context.path("quality_passed").isBoolean() && context.path("quality_passed").asBoolean()
                        && context.path("reconciliation_balanced").isBoolean() && context.path("reconciliation_balanced").asBoolean()
                        && !context.has("publication_quality"), "RESEARCH_QUALITY_REQUIRED");
                for (JsonNode month : context.path("affected_months")) allMonths.add(month.asText());
                require(bundle.path("datasets").size() == 4 && group.path("datasets").size() == 4, "FOUR_DATASETS_REQUIRED");
                var current = new HashSet<String>(); var allIds = new HashSet<String>(); var members = new HashSet<String>();
                var primary = new HashMap<String, Integer>(); var canonicals = new HashSet<String>(); var linkIds = new HashSet<String>();
                var linkRefs = new ArrayList<String>(); var observedMonths = new TreeSet<String>();
                var report = reports.addObject(); report.put("run_id", run); report.put("bundle_sha256", sha(bundleBytes));
                var datasetCounts = report.putObject("dataset_counts");
                for (String name : List.of("source_observation", "reject_record", "canonical_membership", "source_link")) {
                    JsonNode descriptor = group.path("datasets").path(name), ref = bundle.path("datasets").path(name);
                    byte[] manifestBytes = original(descriptor, "manifest_base64");
                    String root = "silver/bundles/" + run + "/" + name + "/";
                    require(sha(manifestBytes).equals(ref.path("sha256").asText()) && (PREFIX + root + "manifest.json").equals(ref.path("manifest_uri").asText()), "MANIFEST_PIN_MISMATCH");
                    JsonNode manifest = JSON.readTree(manifestBytes);
                    require(name.equals(manifest.path("dataset").asText()) && run.equals(manifest.path("run_id").asText())
                            && sha(SCHEMAS.get(name).toString().getBytes(StandardCharsets.UTF_8)).equals(manifest.path("schema_sha256").asText()), "DATASET_SCHEMA_MISMATCH");
                    JsonNode files = manifest.path("files"), sources = descriptor.path("sources");
                    require(files.isArray() && files.size() > 0 && files.size() <= 64 && files.size() == sources.size(), "EXACT_FILES_REQUIRED");
                    long count = 0; var seen = new HashSet<String>();
                    for (int i = 0; i < files.size(); i++) {
                        require(System.nanoTime() < deadline, "READBACK_TIME_BUDGET_EXCEEDED");
                        JsonNode part = files.get(i), source = sources.get(i);
                        String uri = part.path("uri").asText(), relative = safeMember(part.path("relative_path").asText());
                        require(relative.startsWith("bundles/" + run + "/" + name + "/") && (PREFIX + "silver/" + relative).equals(uri)
                                && uri.equals(source.path("uri").asText()) && seen.add(uri), "PAYLOAD_REFERENCE_MISMATCH");
                        require(part.path("byte_size").isIntegralNumber() && part.path("byte_size").canConvertToLong()
                                && part.path("record_count").isIntegralNumber() && part.path("record_count").canConvertToInt(), "INTEGRAL_PAYLOAD_COUNTS_REQUIRED");
                        long size = part.path("byte_size").asLong(-1); int expectedRows = part.path("record_count").asInt(-1);
                        require(size >= 0 && size <= 256L * 1024 * 1024 && expectedRows >= 0 && expectedRows <= 1000000, "PAYLOAD_BOUND_REQUIRED");
                        ZipFile zip = archives.get(source.path("archive_sha256").asText()); require(zip != null, "PINNED_ARCHIVE_REQUIRED");
                        ZipEntry entry = zip.getEntry(safeMember(source.path("member").asText()));
                        require(entry != null && !entry.isDirectory() && entry.getSize() == size, "MEMBER_SIZE_MISMATCH");
                        byte[] raw;
                        try (var stream = zip.getInputStream(entry)) { raw = stream.readNBytes(Math.toIntExact(size + 1)); }
                        require(raw.length == size && sha(raw).equals(part.path("sha256").asText()), "PAYLOAD_SHA_MISMATCH");
                        bytesChecked += raw.length; require(bytesChecked <= 1024L * 1024 * 1024, "BATCH_BYTE_LIMIT");
                        SilverParquetSerializer.verifyParquet(raw, expectedRows, SCHEMAS.get(name));
                        long decoded = 0;
                        try (var reader = ParquetFileReader.open(new SilverParquetSerializer.InMemoryInputFile(raw))) {
                            PageReadStore pages;
                            while ((pages = reader.readNextRowGroup()) != null) {
                                require(System.nanoTime() < deadline, "READBACK_TIME_BUDGET_EXCEEDED");
                                var rowReader = new ColumnIOFactory().getColumnIO(SCHEMAS.get(name)).getRecordReader(pages, new GroupRecordConverter(SCHEMAS.get(name)));
                                for (long n = 0; n < pages.getRowCount(); n++) {
                                    Group row = rowReader.read(); decoded++;
                                    if (name.equals("source_observation")) {
                                        String id = row.getString("source_observation_id", 0); require(!id.isBlank(), "OBSERVATION_ID_REQUIRED"); allIds.add(id);
                                        observedMonths.add(YearMonth.from(Instant.ofEpochMilli(row.getLong("event_time_utc", 0)).atZone(ZoneOffset.UTC)).toString());
                                        if (row.getBoolean("is_current_source_revision", 0)) {
                                            require(current.add(id), "DUPLICATE_CURRENT_OBSERVATION");
                                            require(Set.of("VALID", "WARNING").contains(row.getString("quality_status", 0)), "CURRENT_QUALITY_BLOCKED");
                                        }
                                    } else if (name.equals("canonical_membership")) {
                                        String id = row.getString("source_observation_id", 0), canonical = row.getString("canonical_event_id", 0);
                                        require(current.contains(id) && members.add(id) && !canonical.isBlank(), "MEMBERSHIP_REFERENCE_INVALID"); canonicals.add(canonical);
                                        String status = row.getString("membership_status", 0);
                                        require(Set.of("PRIMARY", "SUPPORTING").contains(status), "MEMBERSHIP_STATUS_INVALID");
                                        if (status.equals("PRIMARY")) primary.merge(canonical, 1, Integer::sum);
                                    } else if (name.equals("source_link")) {
                                        require(linkIds.add(row.getString("source_link_id", 0)), "DUPLICATE_SOURCE_LINK");
                                        linkRefs.add(row.getString("left_observation_id", 0)); linkRefs.add(row.getString("right_observation_id", 0));
                                    }
                                }
                            }
                        }
                        require(decoded == expectedRows, "DECODED_ROW_COUNT_MISMATCH"); count += decoded; rowsDecoded += decoded;
                    }
                    require(count == manifest.path("record_count").asLong(-1) && count == ref.path("record_count").asLong(-1), "DATASET_COUNT_MISMATCH");
                    require(!name.equals("reject_record") || count == 0, "REJECTS_BLOCK_RESEARCH_INPUT"); datasetCounts.put(name, count);
                }
                require(members.equals(current) && primary.keySet().equals(canonicals) && primary.values().stream().allMatch(x -> x == 1), "CANONICAL_MEMBERSHIP_UNBALANCED");
                require(linkRefs.stream().allMatch(allIds::contains), "SOURCE_LINK_REFERENCE_MISSING");
                report.put("current_observation_count", current.size()); report.put("canonical_count", canonicals.size());
                report.set("observed_event_months", JSON.valueToTree(observedMonths)); report.put("status", "PAGES_SCHEMA_COUNTS_RELATIONSHIPS_VERIFIED");
            }
            require(allMonths.equals(months), "AFFECTED_MONTHS_MISMATCH");
            result.put("bytes_checked", bytesChecked); result.put("rows_decoded", rowsDecoded);
            result.put("gold_published", false); result.put("portable_import_ready", false); result.put("official_export_ready", false);
            result.put("s3_keys_verified", false); result.put("full_transitive_closure_verified", false);
            return result;
        } finally { for (ZipFile archive : archives.values()) archive.close(); }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: PortableSilverReadback <plan.json> <expected-sha256> <new-receipt.json>");
        Path output = Path.of(args[2]).toAbsolutePath().normalize();
        require(!Files.exists(output, LinkOption.NOFOLLOW_LINKS), "OUTPUT_EXISTS");
        ObjectNode result = verify(Path.of(args[0]), args[1]);
        Files.write(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(result), StandardOpenOption.CREATE_NEW);
        System.out.println("event=portable_silver_readback_passed groups=" + result.path("groups").size()
                + " rows_decoded=" + result.path("rows_decoded").asLong() + " gold_published=false");
    }
}
