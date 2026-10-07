package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Manifest for a published Silver partition (CON-03 1.0, SLV-08).
 * Describes partition status, file checksums, record counts, and quality metrics.
 */
public record SilverPartitionManifest(
        String schemaVersion,
        String silverStatus,
        String sourceSystem,
        int eventYearUtc,
        int eventMonthUtc,
        String partitionPath,
        String runId,
        int recordCount,
        List<SilverFileMetadata> files,
        Map<String, Integer> qualitySummary,
        Instant publishedAtUtc,
        String writerVersion) implements Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public SilverPartitionManifest {
        Objects.requireNonNull(schemaVersion, "schemaVersion");
        Objects.requireNonNull(silverStatus, "silverStatus");
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        Objects.requireNonNull(partitionPath, "partitionPath");
        Objects.requireNonNull(runId, "runId");
        if (recordCount < 0) {
            throw new IllegalArgumentException("recordCount cannot be negative: " + recordCount);
        }
        files = files != null ? Collections.unmodifiableList(files) : List.of();
        qualitySummary = qualitySummary != null ? Collections.unmodifiableMap(new LinkedHashMap<>(qualitySummary)) : Map.of();
        Objects.requireNonNull(publishedAtUtc, "publishedAtUtc");
        Objects.requireNonNull(writerVersion, "writerVersion");
    }

    /**
     * Serializes this manifest to formatted JSON bytes.
     */
    public byte[] toJsonBytes() throws IOException {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("schema_version", schemaVersion);
        root.put("silver_status", silverStatus);
        root.put("source_system", sourceSystem);
        root.put("event_year_utc", eventYearUtc);
        root.put("event_month_utc", eventMonthUtc);
        root.put("partition_path", partitionPath);
        root.put("run_id", runId);
        root.put("record_count", recordCount);

        ArrayNode filesNode = root.putArray("files");
        for (SilverFileMetadata file : files) {
            ObjectNode fileNode = filesNode.addObject();
            fileNode.put("file_name", file.fileName());
            fileNode.put("relative_path", file.relativePath());
            fileNode.put("sha256", file.sha256());
            fileNode.put("byte_size", file.byteSize());
            fileNode.put("record_count", file.recordCount());
        }

        ObjectNode qualityNode = root.putObject("quality_summary");
        for (Map.Entry<String, Integer> entry : qualitySummary.entrySet()) {
            qualityNode.put(entry.getKey(), entry.getValue());
        }

        root.put("published_at_utc", publishedAtUtc.toString());
        root.put("writer_version", writerVersion);

        return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
    }

    /**
     * Deserializes a manifest from JSON bytes.
     */
    public static SilverPartitionManifest fromJson(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        JsonNode root = OBJECT_MAPPER.readTree(bytes);

        String schemaVersion = root.path("schema_version").asText("1.0");
        String silverStatus = root.path("silver_status").asText();
        String sourceSystem = root.path("source_system").asText();
        int eventYearUtc = root.path("event_year_utc").asInt();
        int eventMonthUtc = root.path("event_month_utc").asInt();
        String partitionPath = root.path("partition_path").asText();
        String runId = root.path("run_id").asText();
        int recordCount = root.path("record_count").asInt();

        List<SilverFileMetadata> files = new ArrayList<>();
        JsonNode filesNode = root.path("files");
        if (filesNode.isArray()) {
            for (JsonNode f : filesNode) {
                files.add(new SilverFileMetadata(
                        f.path("file_name").asText(),
                        f.path("relative_path").asText(),
                        f.path("sha256").asText(),
                        f.path("byte_size").asLong(),
                        f.path("record_count").asInt()));
            }
        }

        Map<String, Integer> qualitySummary = new LinkedHashMap<>();
        JsonNode qualityNode = root.path("quality_summary");
        if (qualityNode.isObject()) {
            qualityNode.fieldNames().forEachRemaining(name -> qualitySummary.put(name, qualityNode.path(name).asInt()));
        }

        Instant publishedAtUtc = Instant.parse(root.path("published_at_utc").asText());
        String writerVersion = root.path("writer_version").asText("slv-08");

        return new SilverPartitionManifest(
                schemaVersion,
                silverStatus,
                sourceSystem,
                eventYearUtc,
                eventMonthUtc,
                partitionPath,
                runId,
                recordCount,
                files,
                qualitySummary,
                publishedAtUtc,
                writerVersion);
    }
}
