package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MlResultImportJobTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURE = Path.of("src/test/resources/ml/results/success");
    private static final ResultBundleValidator.Context CONTEXT = new ResultBundleValidator.Context(
        "ds_fixture", "EXPORTED", "exp_fixture", "WINDOW", "1",
        ResultBundleContractTest.CANDIDATES, null);

    @Test void importOperationIdentityIsNotReplacedWithExperimentIdentity() throws Exception {
        FakeWriter writer = new FakeWriter();
        MlResultImportJob.importBundle(FIXTURE, Files.createTempDirectory("mli03-operation-"), CONTEXT, writer,
            (commit, context, fingerprint) -> Map.of("sequence_membership", 2L, "sequence_summary", 1L),
            "iceberg.ml_qa_test", "mli03-actual-operation");
        assertEquals("mli03-actual-operation", writer.importOperation);
    }

    @Test
    void readsBundleCommitsCandidateAndRerunIsIdempotent() throws Exception {
        Path journal = Files.createTempDirectory("mli03-journal-");
        FakeWriter writer = new FakeWriter();

        ObjectNode first = MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, writer);
        ObjectNode second = MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, writer);

        assertEquals("CANDIDATE", first.path("experiment_status").asText());
        assertEquals(first.toString(), second.toString());
        assertEquals(1, writer.commits);
        assertEquals(2L, first.path("readback").path("sequence_membership").asLong());
        assertEquals(1L, first.path("readback").path("sequence_summary").asLong());
        assertTrue(Files.exists(journal.resolve("experiment-run-exp_fixture.json")));
    }

    @Test
    void rejectsRunReuseWithDifferentFingerprintAndUnexportedDataset() throws Exception {
        Path journal = Files.createTempDirectory("mli03-reuse-");
        ObjectNode existing = JSON.createObjectNode().put("result_bundle_sha256", "different");
        Files.writeString(journal.resolve("experiment-run-exp_fixture.json"), existing.toString());
        assertEquals("IMP_EXPERIMENT_RUN_REUSED", assertThrows(IllegalArgumentException.class,
            () -> MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, new FakeWriter())).getMessage());

        ResultBundleValidator.Context building = new ResultBundleValidator.Context(
            "ds_fixture", "BUILDING", "exp_fixture", "WINDOW", "1", ResultBundleContractTest.CANDIDATES, null);
        assertEquals("IMP_UNKNOWN_DATASET", assertThrows(IllegalArgumentException.class,
            () -> MlResultImportJob.importBundle(FIXTURE, Files.createTempDirectory("mli03-building-"), building, new FakeWriter())).getMessage());
    }

    @Test
    void failedCommitLeavesJournalForSameFingerprintRecovery() throws Exception {
        Path journal = Files.createTempDirectory("mli03-recovery-");
        FakeWriter failing = new FakeWriter();
        failing.failCommit = true;
        assertThrows(IOException.class, () -> MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, failing));
        try (var attempts = Files.list(journal.resolve("attempts"))) {
            assertTrue(attempts.findAny().isPresent());
        }
        assertEquals("IMPORT_VALIDATING", JSON.readTree(Files.readString(journal.resolve("experiment-run-exp_fixture.json")))
            .path("experiment_status").asText());

        FakeWriter retry = new FakeWriter();
        ObjectNode candidate = MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, retry);
        assertEquals("CANDIDATE", candidate.path("experiment_status").asText());
        assertEquals(1, retry.commits);
    }

    @Test
    void pilotManifestCannotBePresentedAsAnOfficialBundle() throws Exception {
        Path bundle = Files.createTempDirectory("mli03-pilot-");
        try (var files = Files.walk(FIXTURE)) {
            files.forEach(source -> {
                try {
                    Path target = bundle.resolve(FIXTURE.relativize(source).toString());
                    if (Files.isDirectory(source)) Files.createDirectories(target);
                    else Files.copy(source, target);
                } catch (Exception error) { throw new RuntimeException(error); }
            });
        }
        Files.writeString(bundle.resolve("pilot_result_manifest.json"), "{}\n");
        assertEquals("IMP_UNSAFE_ARTIFACT_METADATA", assertThrows(IllegalArgumentException.class,
            () -> MlResultBundleReader.read(bundle)).getMessage());
        Files.delete(bundle.resolve("pilot_result_manifest.json"));
        Files.writeString(bundle.resolve("_SUCCESS.json"), "{\"status\":\"DEVELOPMENT_PILOT\"}\n");
        assertEquals("IMP_INCOMPLETE_BUNDLE", assertThrows(IllegalArgumentException.class,
            () -> MlResultImportJob.importBundle(bundle, Files.createTempDirectory("mli03-renamed-pilot-"),
                CONTEXT, new FakeWriter())).getMessage());
    }

    @Test
    void verificationFailureDoesNotCreateServingCandidate() throws Exception {
        Path journal = Files.createTempDirectory("mli03-trino-blocker-");
        FakeWriter writer = new FakeWriter();
        assertEquals("IMP_TRINO_QUALITY_BLOCKER", assertThrows(IOException.class, () ->
            MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, writer,
                (commit, context, fingerprint) -> { throw new IOException("IMP_TRINO_QUALITY_BLOCKER"); }, "iceberg.ml_qa_test")).getMessage());
        JsonNode state = JSON.readTree(Files.readString(journal.resolve("experiment-run-exp_fixture.json")));
        assertEquals("REJECTED", state.path("experiment_status").asText());
        assertFalse(state.path("experiment_status").asText().equals("CANDIDATE"));
    }

    @Test
    void invalidRetryKeepsSuccessfulCanonicalJournalAndRecordsSeparateAttempt() throws Exception {
        Path journal = Files.createTempDirectory("mli03-invalid-retry-");
        FakeWriter writer = new FakeWriter();
        ObjectNode first = MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, writer);
        Path invalid = Files.createTempDirectory("mli03-invalid-bundle-");
        try (var paths = Files.walk(FIXTURE)) {
            paths.forEach(source -> {
                try {
                    Path target = invalid.resolve(FIXTURE.relativize(source).toString());
                    if (Files.isDirectory(source)) Files.createDirectories(target);
                    else Files.copy(source, target);
                } catch (Exception error) { throw new RuntimeException(error); }
            });
        }
        ObjectNode success = (ObjectNode) JSON.readTree(Files.readString(invalid.resolve("_SUCCESS.json")));
        ((ObjectNode) success.path("checksums")).put("experiment_config.json", "0".repeat(64));
        Files.writeString(invalid.resolve("_SUCCESS.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(success));

        assertThrows(IllegalArgumentException.class, () -> MlResultImportJob.importBundle(invalid, journal, CONTEXT, writer));
        JsonNode canonical = JSON.readTree(Files.readString(journal.resolve("experiment-run-exp_fixture.json")));
        assertEquals("CANDIDATE", canonical.path("experiment_status").asText());
        assertEquals(first.path("result_bundle_sha256").asText(), canonical.path("result_bundle_sha256").asText());
        assertTrue(Files.list(journal.resolve("attempts")).findAny().isPresent());

        ObjectNode config = (ObjectNode) JSON.readTree(Files.readString(invalid.resolve("experiment_config.json")));
        config.put("random_seed", config.path("random_seed").asLong() + 1);
        byte[] configBytes = (JSON.writerWithDefaultPrettyPrinter().writeValueAsString(config) + "\n")
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(invalid.resolve("experiment_config.json"), configBytes);
        ObjectNode validChangedSuccess = (ObjectNode) JSON.readTree(Files.readString(invalid.resolve("_SUCCESS.json")));
        ((ObjectNode) validChangedSuccess.path("checksums")).put("experiment_config.json", ResultBundleContract.sha256(configBytes));
        Files.writeString(invalid.resolve("_SUCCESS.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(validChangedSuccess) + "\n");

        assertEquals("IMP_EXPERIMENT_RUN_REUSED", assertThrows(IllegalArgumentException.class,
            () -> MlResultImportJob.importBundle(invalid, journal, CONTEXT, writer)).getMessage());
        JsonNode afterChangedFingerprint = JSON.readTree(Files.readString(journal.resolve("experiment-run-exp_fixture.json")));
        assertEquals("CANDIDATE", afterChangedFingerprint.path("experiment_status").asText());
        assertEquals(canonical.path("result_bundle_sha256").asText(), afterChangedFingerprint.path("result_bundle_sha256").asText());

        ObjectNode retry = MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, writer);
        assertEquals("CANDIDATE", retry.path("experiment_status").asText());
        assertEquals(1, writer.commits);
        assertEquals(canonical.path("result_bundle_sha256").asText(), retry.path("result_bundle_sha256").asText());
    }

    @Test
    void completedJournalMustRemainInTheSameNamespace() throws Exception {
        Path journal = Files.createTempDirectory("mli03-namespace-");
        ObjectNode existing = JSON.createObjectNode().put("result_bundle_sha256", "different");
        // The mismatch is checked before any fast-path readback and cannot be silently reused.
        Files.writeString(journal.resolve("experiment-run-exp_fixture.json"), existing.toString());
        assertEquals("IMP_EXPERIMENT_RUN_REUSED", assertThrows(IllegalArgumentException.class, () ->
            MlResultImportJob.importBundle(FIXTURE, journal, CONTEXT, new FakeWriter(),
                (commit, context, fingerprint) -> Map.of("sequence_membership", 2L, "sequence_summary", 1L), "iceberg.ml_qa_other")).getMessage());
    }

    private static final class FakeWriter implements MlIcebergWriter {
        int commits;
        boolean failCommit;
        String fingerprint;
        String status;
        String importOperation;
        ResultCommit pinned;

        @Override public Reservation reserveExperiment(ResultBundleValidator.DecodedBundle bundle,
                                                       ResultBundleValidator.Context context,
                                                       String resultBundleSha256) {
            importOperation = context.experimentMetadata().importRunId();
            if (fingerprint != null && !fingerprint.equals(resultBundleSha256))
                throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
            if ("REJECTED".equals(status)) throw new IllegalStateException("IMP_REJECTED_RUN_TERMINAL");
            if ("CANDIDATE".equals(status)) return new Reservation(status,
                pinned.membershipSnapshot(), pinned.summarySnapshot(), 103L);
            fingerprint = resultBundleSha256;
            status = "IMPORT_VALIDATING";
            return new Reservation(status, 0L, 0L, 0L);
        }

        @Override public ResultCommit commitResults(ResultBundleValidator.DecodedBundle bundle,
                                                    ResultBundleValidator.Context context,
                                                    String resultBundleSha256) throws Exception {
            Commit committed = commit(bundle, context, resultBundleSha256);
            pinned = new ResultCommit(committed.membershipSnapshot(), committed.summarySnapshot());
            return pinned;
        }

        @Override public long publishCandidate(ResultBundleValidator.DecodedBundle bundle,
                                               ResultBundleValidator.Context context,
                                               String resultBundleSha256, ResultCommit commit) {
            status = "CANDIDATE";
            pinned = commit;
            return 103L;
        }

        @Override public Commit commit(ResultBundleValidator.DecodedBundle bundle,
                                       ResultBundleValidator.Context context, String fingerprint) throws Exception {
            if (failCommit) throw new IOException("synthetic commit failure");
            commits++;
            return new Commit(101L, 102L, 103L);
        }

        @Override public Map<String, Long> verify(Commit commit, ResultBundleValidator.Context context) {
            Map<String, Long> counts = new LinkedHashMap<>();
            counts.put("sequence_membership", 2L);
            counts.put("sequence_summary", 1L);
            counts.put("experiment_run", 1L);
            return counts;
        }
    }
}
