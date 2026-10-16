package ie212.earthquake.spark.ml;

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
        assertTrue(Files.exists(journal.resolve("rejected-exp_fixture.json")));
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
    }

    private static final class FakeWriter implements MlIcebergWriter {
        int commits;
        boolean failCommit;

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
