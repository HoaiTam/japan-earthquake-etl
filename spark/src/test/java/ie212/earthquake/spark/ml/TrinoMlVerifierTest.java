package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import ie212.earthquake.spark.gold.GoldPublisher;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TrinoMlVerifierTest {
    private static final ResultBundleValidator.Context CONTEXT = new ResultBundleValidator.Context(
        "ds_fixture", "EXPORTED", "exp_fixture", "WINDOW", "1", ResultBundleContractTest.CANDIDATES, null);

    @Test void positiveButIncompleteCountCannotPublish() {
        var verifier = new TrinoMlVerifier(query -> count(1), "iceberg.ml_qa_test", () -> { });
        assertEquals("IMP_TRINO_QUALITY_BLOCKER", assertThrows(IOException.class, () ->
            verifier.verify(new MlIcebergWriter.ResultCommit(11L, 12L), CONTEXT, "a".repeat(64))).getMessage());
    }

    @Test void membershipMustBelongToSameDataset() {
        var verifier = new TrinoMlVerifier(query -> {
            if (query.contains("HAVING")) return count(0);
            if (query.contains("$snapshots")) return count(1);
            if (query.contains("sequence_membership")) return count(query.contains("dataset_id=") ? 1 : 2);
            return count(1);
        }, "iceberg.ml_qa_test", () -> { });
        assertEquals("IMP_TRINO_LINEAGE_MISMATCH", assertThrows(IOException.class, () ->
            verifier.verify(new MlIcebergWriter.ResultCommit(11L, 12L), CONTEXT, "a".repeat(64))).getMessage());
    }

    private static GoldPublisher.Result count(long value) {
        return new GoldPublisher.Result(List.of(), List.of(new ObjectMapper().createArrayNode().add(value)));
    }
}
