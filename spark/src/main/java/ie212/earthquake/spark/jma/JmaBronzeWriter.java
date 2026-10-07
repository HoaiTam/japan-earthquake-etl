package ie212.earthquake.spark.jma;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.usgs.BronzeObjectStore;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/** Immutable raw ZIP publication. The manifest is the final commit point, never a staging marker. */
public final class JmaBronzeWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final BronzeObjectStore store;
    private final JmaArchiveValidator validator;

    public JmaBronzeWriter(BronzeObjectStore store) {
        this(store, new JmaArchiveValidator());
    }

    public JmaBronzeWriter(BronzeObjectStore store, JmaArchiveValidator validator) {
        this.store = Objects.requireNonNull(store, "store");
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    public JmaBronzeWriteResult write(JmaBronzeWriteRequest input) throws IOException {
        Objects.requireNonNull(input, "input");
        byte[] archive = input.archive();
        String digest = sha256(archive);
        boolean checksumValid = digest.equals(input.expectedSha256());
        JmaArchiveValidationResult structure = validator.validate(archive, input.entry().memberName());
        String reason = null;
        if (input.http().statusCode() < 200 || input.http().statusCode() >= 300) {
            reason = "HTTP_STATUS_" + input.http().statusCode();
        } else if (input.http().contentType() == null
                || !"application/zip".equalsIgnoreCase(input.http().contentType().split(";", 2)[0].trim())) {
            reason = "UNEXPECTED_CONTENT_TYPE";
        } else if (!checksumValid) {
            reason = "CHECKSUM_MISMATCH";
        } else if (archive.length != input.expectedLengthBytes()
                || (input.http().contentLengthBytes() != null
                    && archive.length != input.http().contentLengthBytes())) {
            reason = "SOURCE_LENGTH_MISMATCH";
        } else if (!structure.valid()) {
            reason = structure.reason();
        }
        boolean ready = reason == null;
        String base = ready
                ? "bronze/jma/year=" + input.entry().year() + "/catalog_release=" + input.catalogRelease()
                : "bronze/_quarantine/jma";
        base += "/ingest_date=" + input.ingestDateUtc() + "/run_id=" + input.runId()
                + "/attempt=" + String.format(Locale.ROOT, "%02d", input.attempt()) + "/";
        String rawKey = base + (ready ? "archive.zip" : "payload.bin");
        String manifestKey = base + (ready ? "manifest.json" : "failure_manifest.json");
        ObjectNode manifest = manifest(input, archive.length, digest, rawKey, manifestKey,
                ready, reason, structure, checksumValid);
        // A prior manifest with different logical/provenance metadata is not a successful retry.
        boolean reused = store.exists(manifestKey);
        if (reused) {
            verifyManifest(manifestKey, manifest);
        }
        ensureRaw(rawKey, archive, ready ? "application/zip" : "application/octet-stream", digest);
        // Raw has been read back and verified. Publish metadata last, including on recovery after a partial write.
        if (!reused) {
            try {
                store.putIfAbsent(manifestKey, JSON.writeValueAsBytes(manifest), "application/json");
            } catch (FileAlreadyExistsException exception) {
                reused = true;
            }
        }
        verifyManifest(manifestKey, manifest);
        return new JmaBronzeWriteResult(ready ? "BronzeReady" : "Rejected", rawKey, manifestKey,
                digest, structure.recordCount(), reused, reason);
    }

    private void ensureRaw(String key, byte[] bytes, String type, String digest) throws IOException {
        boolean existed = store.exists(key);
        if (!existed) {
            try {
                store.putIfAbsent(key, bytes, type);
            } catch (FileAlreadyExistsException exception) {
                existed = true;
            }
        }
        byte[] readback = store.read(key);
        if (readback.length != bytes.length || !digest.equals(sha256(readback))) {
            throw new IOException((existed ? "AMBIGUOUS_OVERWRITE" : "RAW_READBACK_MISMATCH") + ": " + key);
        }
    }

    private void verifyManifest(String key, ObjectNode expected) throws IOException {
        JsonNode existing = JSON.readTree(store.read(key));
        if (existing == null || !existing.isObject()) {
            throw new IOException("AMBIGUOUS_OVERWRITE: invalid manifest at " + key);
        }
        ObjectNode actual = (ObjectNode) existing;
        // Compare the persisted JSON representation: small LongNodes become IntNodes on readback.
        ObjectNode comparable = (ObjectNode) JSON.readTree(JSON.writeValueAsBytes(expected));
        // A later check of identical source bytes retains the original retrieval time and citation.
        String retrievedAt = actual.path("retrieved_at_utc").asText(null);
        try {
            Instant.parse(retrievedAt);
        } catch (RuntimeException exception) {
            throw new IOException("AMBIGUOUS_OVERWRITE: invalid retrieval timestamp at " + key, exception);
        }
        comparable.put("retrieved_at_utc", retrievedAt);
        ((ObjectNode) comparable.path("provenance")).put("citation_text", citationText(
                comparable.path("request").path("url").asText(), retrievedAt,
                comparable.path("catalog_release").asText()));
        if (!actual.equals(comparable)) {
            throw new IOException("AMBIGUOUS_OVERWRITE: manifest metadata differs at " + key);
        }
    }

    private ObjectNode manifest(JmaBronzeWriteRequest input, int length, String digest,
            String rawKey, String manifestKey, boolean ready, String reason,
            JmaArchiveValidationResult structure, boolean checksumValid) {
        JmaArchiveEntry entry = input.entry();
        ObjectNode root = JSON.createObjectNode();
        root.put("manifest_version", "1.0");
        root.put("manifest_id", "m-" + sha256(manifestKey.getBytes(java.nio.charset.StandardCharsets.UTF_8)).substring(0, 24));
        root.put("bronze_status", ready ? "BronzeReady" : "Rejected");
        root.put("source_system", "JMA_BULLETIN");
        root.put("source_kind", "annual_archive");
        root.put("raw_object_uri", store.uriForKey(rawKey));
        root.put("raw_object_key", rawKey);
        root.put("media_type", "application/zip");
        root.put("content_encoding", "identity");
        root.put("content_length_bytes", length);
        root.put("sha256", digest);
        root.put("run_id", input.runId());
        root.put("attempt", input.attempt());
        root.put("ingest_date_utc", input.ingestDateUtc().toString());
        root.put("retrieved_at_utc", input.retrievedAtUtc().toString());
        root.put("is_backfill", input.backfill());
        root.put("logical_run_key", input.logicalRunKey());
        root.put("catalog_release", input.catalogRelease());
        root.put("record_count_estimate", structure.recordCount());
        ObjectNode interval = root.putObject("data_interval");
        interval.put("year", entry.year());
        interval.put("segment", entry.segment());
        interval.put("native_timezone", "Asia/Tokyo");
        interval.put("native_start", entry.nativeStartJst());
        interval.put("native_end", entry.nativeEndJst());
        interval.put("interval_semantics", "[start,end)");
        ObjectNode request = root.putObject("request");
        request.put("method", "GET");
        request.put("url", entry.sourceUrl().toString());
        request.put("path", entry.archiveName());
        request.put("timeout_ms", input.requestTimeoutMs());
        ObjectNode response = root.putObject("response");
        response.put("http_status", input.http().statusCode());
        response.put("content_type", input.http().contentType());
        response.put("content_length_header", input.http().contentLengthBytes());
        response.put("etag", input.http().etag());
        response.put("last_modified", input.http().lastModified());
        ObjectNode validation = root.putObject("validation");
        validation.put("object_write_completed", true);
        validation.put("raw_readback_verified", true);
        validation.put("checksum_verified", checksumValid);
        validation.put("source_structure_valid", structure.valid());
        validation.put("manifest_consistent", ready);
        ObjectNode writer = root.putObject("writer");
        writer.put("component", "jma-bronze-writer");
        writer.put("version", "jma-03-v1");
        ObjectNode lineage = root.putObject("lineage");
        lineage.putNull("retry_of_manifest_uri");
        lineage.putNull("supersedes_manifest_uri");
        lineage.put("staged_object_uri", input.stagedObjectUri());
        ObjectNode provenance = root.putObject("provenance");
        provenance.put("dataset_title", "The Seismological Bulletin of Japan — Hypocenters");
        provenance.put("inventory_version", entry.inventoryVersion());
        provenance.put("archive_name", entry.archiveName());
        provenance.put("member_name", entry.memberName());
        provenance.put("record_format", entry.recordFormat());
        provenance.put("record_length_bytes", 96);
        provenance.put("catalog_era", entry.catalogEra());
        provenance.put("geodetic_datum", "Japanese Geodetic Datum 2000");
        provenance.put("index_url", "https://www.data.jma.go.jp/eqev/data/bulletin/hypo.html");
        provenance.put("final_url", input.http().finalUri().toString());
        provenance.put("citation_version", "jma-01-v1");
        provenance.put("citation_text", citationText(entry.sourceUrl().toString(),
                input.retrievedAtUtc().toString(), input.catalogRelease()));
        provenance.put("terms_url", "https://www.jma.go.jp/jma/en/copyright.html");
        provenance.put("update_url", "https://www.data.jma.go.jp/eqev/data/bulletin/update_e.html");
        provenance.put("errata_url", "https://www.data.jma.go.jp/eqev/data/bulletin/errata.html");
        if (reason != null) {
            root.put("failure_reason", reason);
        }
        return root;
    }

    private static String citationText(String sourceUrl, String retrievedAt, String release) {
        return "Source: Japan Meteorological Agency (JMA), The Seismological Bulletin of Japan — Hypocenters, "
                + sourceUrl + ", retrieved " + retrievedAt + ", catalog release " + release + ".";
    }

    public static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }
}
