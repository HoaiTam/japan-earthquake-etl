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

/** Writes a small auditable quality summary without copying raw payloads. */
public final class SilverQualitySummaryWriter {
    private static final ObjectMapper JSON = new ObjectMapper();

    public Path write(Path target, SilverQualityResult result, String sourceSystem) throws IOException {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(sourceSystem, "sourceSystem");
        ObjectNode root = JSON.createObjectNode();
        root.put("summary_version", "slv-05-v1");
        root.put("source_system", sourceSystem);
        root.put("ingest_run_id", result.ingestRunId());
        root.put("parsed_count", result.parsedCount());
        root.put("valid_count", result.validCount());
        root.put("rejected_count", result.rejectedCount());
        root.put("publish_blocked", result.publishBlocked());
        ObjectNode reasons = root.putObject("reason_counts");
        for (Map.Entry<String, Long> entry : result.reasonCounts().entrySet()) {
            reasons.put(entry.getKey(), entry.getValue());
        }
        ArrayNode rejected = root.putArray("rejected_records");
        for (SilverRejectRecord record : result.rejectedRecords()) {
            ObjectNode item = rejected.addObject();
            SilverObservation observation = record.observation();
            item.put("reject_stage", record.rejectStage());
            item.put("source_record_key_candidate", observation == null ? null : observation.sourceRecordKey());
            item.put("bronze_manifest_id", observation == null ? null : observation.bronzeManifestId());
            item.put("raw_object_uri", observation == null ? null : observation.rawObjectUri());
            item.put("raw_record_locator", observation == null ? null : observation.rawRecordLocator());
            ArrayNode codes = item.putArray("reject_reason_codes");
            record.reasonCodes().forEach(codes::add);
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
