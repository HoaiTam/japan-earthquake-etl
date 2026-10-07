package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Writes a small auditable quality summary without copying raw payloads. */
public final class SilverQualitySummaryWriter {
    private static final ObjectMapper JSON = new ObjectMapper();

    public Path write(Path target, SilverQualityResult result, String sourceSystem) throws IOException {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        if (!("USGS".equals(sourceSystem) || "JMA_BULLETIN".equals(sourceSystem))
                || result.validObservations().stream().anyMatch(record -> !sourceSystem.equals(record.sourceSystem()))
                || result.rejectedRecords().stream().anyMatch(record -> !sourceSystem.equals(record.sourceSystem()))) {
            throw new IllegalArgumentException("quality summary must describe one exact source system");
        }
        ObjectNode root = JSON.createObjectNode();
        root.put("summary_version", "slv-05-v1");
        root.put("source_system", sourceSystem);
        root.put("ingest_run_id", result.ingestRunId());
        root.put("parsed_count", result.parsedCount());
        root.put("valid_count", result.validCount());
        root.put("rejected_count", result.rejectedCount());
        root.put("publish_blocked", result.publishBlocked());
        ObjectNode reasons = root.putObject("reason_counts");
        for (Map.Entry<String, Long> entry : new TreeMap<>(result.reasonCounts()).entrySet()) {
            reasons.put(entry.getKey(), entry.getValue());
        }
        ArrayNode rejected = root.putArray("rejected_records");
        for (SilverRejectRecord record : result.rejectedRecords()) {
            ObjectNode item = rejected.addObject();
            item.put("schema_version", record.schemaVersion());
            item.put("source_system", record.sourceSystem());
            item.put("reject_stage", record.rejectStage());
            item.put("source_record_key_candidate", record.sourceRecordKeyCandidate());
            item.put("bronze_manifest_id", record.bronzeManifestId());
            item.put("raw_object_uri", record.rawObjectUri());
            item.put("raw_sha256", record.rawSha256());
            item.put("raw_record_locator", record.rawRecordLocator());
            item.put("raw_record_hash", record.rawRecordHash());
            item.put("ingest_run_id", record.ingestRunId());
            item.put("parser_version", record.parserVersion());
            item.put("rejected_at_utc", record.rejectedAtUtc().toString());
            ArrayNode codes = item.putArray("reject_reason_codes");
            record.rejectReasonCodes().forEach(codes::add);
        }
        Files.createDirectories(target.toAbsolutePath().normalize().getParent());
        Path temporary = target.resolveSibling("." + target.getFileName() + ".part");
        Files.writeString(temporary, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n");
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }
}
