package vn.edu.uit.ie212.earthquake.spark.usgs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Validates and publishes immutable USGS raw response plus Bronze manifest. */
public final class UsgsBronzeWriter {
    private static final String RAW_CONTENT_TYPE = "application/geo+json";
    private static final String MANIFEST_CONTENT_TYPE = "application/json";
    private static final String MANIFEST_VERSION = "1.0";

    private final BronzeObjectStore objectStore;
    private final UsgsGeoJsonValidator validator;
    private final ObjectMapper objectMapper;
    private final String writerVersion;

    public UsgsBronzeWriter(BronzeObjectStore objectStore) {
        this(objectStore, new UsgsGeoJsonValidator(), new ObjectMapper(), "usg-03");
    }

    UsgsBronzeWriter(
            BronzeObjectStore objectStore,
            UsgsGeoJsonValidator validator,
            ObjectMapper objectMapper,
            String writerVersion) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.writerVersion = Objects.requireNonNull(writerVersion, "writerVersion");
    }

    public UsgsBronzeWriteResult write(UsgsBronzeWriteRequest input) throws IOException {
        Objects.requireNonNull(input, "input");
        UsgsHttpResponse response = input.response();
        byte[] payload = response.body();
        String sha256 = sha256(payload);
        UsgsGeoJsonValidationResult validation = validator.validate(payload);
        boolean httpSuccess = response.statusCode() >= 200 && response.statusCode() < 300;
        if (!httpSuccess || !validation.valid()) {
            String reason = !httpSuccess
                    ? "HTTP_STATUS_" + response.statusCode()
                    : validation.reason();
            return reject(input, payload, sha256, reason);
        }

        String rawKey = rawKey(input.ingestDateUtc(), input.runId(), input.attempt(), "response.geojson");
        String manifestKey = rawKey(input.ingestDateUtc(), input.runId(), input.attempt(), "manifest.json");
        boolean reused = ensureRawObject(rawKey, payload, sha256, RAW_CONTENT_TYPE);
        byte[] readback = objectStore.read(rawKey);
        if (!sha256(readback).equals(sha256) || readback.length != payload.length) {
            throw new IOException("Bronze raw readback checksum or length mismatch: " + rawKey);
        }

        byte[] manifest = buildManifest(
                input,
                response,
                rawKey,
                manifestKey,
                sha256,
                validation.featureCount(),
                "BronzeReady",
                null,
                true,
                true,
                true,
                true,
                true);
        ensureManifest(manifestKey, manifest, sha256, rawKey, "BronzeReady");
        return new UsgsBronzeWriteResult(
                "BronzeReady",
                rawKey,
                manifestKey,
                sha256,
                validation.featureCount(),
                reused,
                null);
    }

    private UsgsBronzeWriteResult reject(
            UsgsBronzeWriteRequest input,
            byte[] payload,
            String sha256,
            String reason)
            throws IOException {
        String rawKey = rawKey(input.ingestDateUtc(), input.runId(), input.attempt(), "payload.bin", true);
        String manifestKey = rawKey(input.ingestDateUtc(), input.runId(), input.attempt(), "failure_manifest.json", true);
        boolean reused = ensureRawObject(rawKey, payload, sha256, "application/octet-stream");
        UsgsHttpResponse response = input.response();
        byte[] manifest = buildManifest(
                input,
                response,
                rawKey,
                manifestKey,
                sha256,
                null,
                "Rejected",
                reason,
                false,
                true,
                true,
                false,
                false);
        ensureManifest(manifestKey, manifest, sha256, rawKey, "Rejected");
        return new UsgsBronzeWriteResult(
                "Rejected",
                rawKey,
                manifestKey,
                sha256,
                null,
                reused,
                reason);
    }

    private boolean ensureRawObject(
            String key,
            byte[] payload,
            String expectedSha256,
            String contentType)
            throws IOException {
        if (objectStore.exists(key)) {
            verifyExistingObject(key, expectedSha256, payload.length);
            return true;
        }
        try {
            objectStore.putIfAbsent(key, payload, contentType);
        } catch (FileAlreadyExistsException exception) {
            verifyExistingObject(key, expectedSha256, payload.length);
            return true;
        }
        verifyExistingObject(key, expectedSha256, payload.length);
        return false;
    }

    private void verifyExistingObject(String key, String expectedSha256, int expectedLength)
            throws IOException {
        byte[] existing = objectStore.read(key);
        if (existing.length != expectedLength || !sha256(existing).equals(expectedSha256)) {
            throw new IOException("AMBIGUOUS_OVERWRITE: Bronze object differs at key " + key);
        }
    }

    private void ensureManifest(
            String manifestKey,
            byte[] manifest,
            String expectedSha256,
            String expectedRawKey,
            String expectedStatus)
            throws IOException {
        if (!objectStore.exists(manifestKey)) {
            try {
                objectStore.putIfAbsent(manifestKey, manifest, MANIFEST_CONTENT_TYPE);
                return;
            } catch (FileAlreadyExistsException ignored) {
                // Another writer won the immutable create race; verify its metadata below.
            }
        }

        JsonNode existing = readJson(objectStore.read(manifestKey));
        if (!expectedStatus.equals(existing.path("bronze_status").asText())
                || !expectedSha256.equals(existing.path("sha256").asText())
                || !expectedRawKey.equals(existing.path("raw_object_key").asText())) {
            throw new IOException("AMBIGUOUS_OVERWRITE: manifest differs at key " + manifestKey);
        }
    }

    private byte[] buildManifest(
            UsgsBronzeWriteRequest input,
            UsgsHttpResponse response,
            String rawKey,
            String manifestKey,
            String sha256,
            Integer recordCount,
            String status,
            String rejectionReason,
            boolean objectWriteCompleted,
            boolean rawReadbackVerified,
            boolean checksumVerified,
            boolean sourceStructureValid,
            boolean manifestConsistent)
            throws IOException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("manifest_version", MANIFEST_VERSION);
        root.put("manifest_id", "m-" + input.runId() + "-" + sha256.substring(0, 16));
        root.put("bronze_status", status);
        root.put("source_system", "USGS");
        root.put("source_kind", "event_api");
        root.put("raw_object_uri", objectStore.uriForKey(rawKey));
        root.put("raw_object_key", rawKey);
        root.put("media_type", RAW_CONTENT_TYPE);
        root.put("content_encoding", "identity");
        root.put("content_length_bytes", response.bodyLengthBytes());
        root.put("sha256", sha256);
        root.put("run_id", input.runId());
        root.put("attempt", input.attempt());
        root.put("ingest_date_utc", input.ingestDateUtc().toString());
        root.put("retrieved_at_utc", input.retrievedAtUtc().toString());
        root.put("is_backfill", input.backfill());
        root.put("logical_run_key", input.logicalRunKey());

        ObjectNode interval = root.putObject("data_interval");
        interval.put("window_start_utc", response.request().windowStartUtc().toString());
        interval.put("window_end_utc", response.request().windowEndExclusiveUtc().toString());
        interval.put("interval_semantics", "[start,end)");

        ObjectNode requestNode = root.putObject("request");
        requestNode.put("method", "GET");
        requestNode.put("url", response.request().uri().toString());
        requestNode.set("query", queryNode(response.request().uri()));
        requestNode.put("timeout_ms", input.requestTimeoutMs());

        ObjectNode responseNode = root.putObject("response");
        responseNode.put("http_status", response.statusCode());
        putHeader(responseNode, response, "content-type", "content_type");
        putHeader(responseNode, response, "content-length", "content_length_header");
        putHeader(responseNode, response, "etag", "etag");
        putHeader(responseNode, response, "last-modified", "last_modified");
        putHeader(responseNode, response, "date", "server_date");

        root.putNull("catalog_release");
        if (recordCount == null) {
            root.putNull("record_count_estimate");
        } else {
            root.put("record_count_estimate", recordCount);
        }
        ObjectNode validation = root.putObject("validation");
        validation.put("object_write_completed", objectWriteCompleted);
        validation.put("raw_readback_verified", rawReadbackVerified);
        validation.put("checksum_verified", checksumVerified);
        validation.put("source_structure_valid", sourceStructureValid);
        validation.put("manifest_consistent", manifestConsistent);
        ObjectNode writer = root.putObject("writer");
        writer.put("component", "usgs-bronze-writer");
        writer.put("version", writerVersion);
        ObjectNode lineage = root.putObject("lineage");
        lineage.putNull("retry_of_manifest_uri");
        lineage.putNull("supersedes_manifest_uri");
        if (rejectionReason != null) {
            root.put("failure_reason", rejectionReason);
        }
        return objectMapper.writeValueAsBytes(root);
    }

    private ObjectNode queryNode(URI uri) {
        ObjectNode query = objectMapper.createObjectNode();
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            return query;
        }
        for (String parameter : rawQuery.split("&")) {
            String[] pair = parameter.split("=", 2);
            String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
            String value = pair.length == 1
                    ? ""
                    : URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
            query.put(key, value);
        }
        return query;
    }

    private static void putHeader(
            ObjectNode responseNode,
            UsgsHttpResponse response,
            String header,
            String field) {
        response.headers().entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase(header))
                .findFirst()
                .ifPresentOrElse(
                        entry -> responseNode.put(field, String.join(",", entry.getValue())),
                        () -> responseNode.putNull(field));
    }

    private static String rawKey(
            LocalDate ingestDate,
            String runId,
            int attempt,
            String filename) {
        return rawKey(ingestDate, runId, attempt, filename, false);
    }

    private static String rawKey(
            LocalDate ingestDate,
            String runId,
            int attempt,
            String filename,
            boolean quarantine) {
        String prefix = quarantine ? "bronze/_quarantine/usgs" : "bronze/usgs";
        return String.format(
                java.util.Locale.ROOT,
                "%s/ingest_date=%s/run_id=%s/attempt=%02d/%s",
                prefix,
                ingestDate,
                runId,
                attempt,
                filename);
    }

    private static JsonNode readJson(byte[] payload) throws IOException {
        return new ObjectMapper().readTree(payload);
    }

    static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM must provide SHA-256", exception);
        }
    }
}
