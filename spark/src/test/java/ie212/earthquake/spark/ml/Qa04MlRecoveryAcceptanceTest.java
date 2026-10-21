package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Qa04MlRecoveryAcceptanceTest {
    @TempDir Path root;
    private Path fixture() {
        Path path=Path.of("src/test/resources/ml/results/success");
        return Files.isDirectory(path)?path:Path.of("spark/src/test/resources/ml/results/success");
    }
    @Test void freshExperimentBundleKeepsExactParquetSchemaAndAllIdentities() throws Exception {
        byte[] original=Files.readAllBytes(fixture().resolve("_SUCCESS.json"));
        Path bundle=Qa04MlRecoveryAcceptance.bundle(root,"exp_recovery_test",fixture());
        var decoded=MlResultBundleReader.read(bundle);
        assertEquals("exp_recovery_test",decoded.success().path("experiment_run_id").asText());
        for(var row:decoded.memberships())assertEquals("exp_recovery_test",row.path("experiment_run_id").asText());
        for(var row:decoded.summaries())assertEquals("exp_recovery_test",row.path("experiment_run_id").asText());
        var context=new ResultBundleValidator.Context("ds_fixture","EXPORTED","exp_recovery_test","WINDOW","1",
            List.of(new ResultBundleValidator.Candidate("main","main","MAINSHOCK"),new ResultBundleValidator.Candidate("main","post","POST")),null);
        assertEquals(64,new ResultBundleContract().validate(decoded,context).bundleSha256().length());
        assertArrayEquals(original,Files.readAllBytes(fixture().resolve("_SUCCESS.json")));
    }
    @Test void unsafeFixtureIdentityIsRejectedBeforeCreatingDirectory() {
        assertThrows(java.io.IOException.class,()->Qa04MlRecoveryAcceptance.bundle(root,"../exp_escape",fixture()));
        assertEquals(0,root.toFile().list().length);
    }
    @Test void productionNamespacesAreRejectedBeforeSparkOrStorage() {
        for(String ns:List.of("iceberg.ml","iceberg.gold","iceberg.qa04_ml_short"))
            assertThrows(java.io.IOException.class,()->Qa04MlRecoveryAcceptance.accept(null,null,root,ns,()->{}));
    }
    @Test void schemaFaultDoesNotSubstituteAParquetFileWithAnotherExperimentIdentity() throws Exception {
        Path bundle=Qa04MlRecoveryAcceptance.bundle(root,"exp_bad_schema",fixture());
        Qa04MlRecoveryAcceptance.corruptSchema(bundle);
        var decoded=MlResultBundleReader.read(bundle);
        var context=new ResultBundleValidator.Context("ds_fixture","EXPORTED","exp_bad_schema","WINDOW","1",
            List.of(new ResultBundleValidator.Candidate("main","main","MAINSHOCK"),new ResultBundleValidator.Candidate("main","post","POST")),null);
        assertEquals("IMP_SCHEMA_MISMATCH",assertThrows(IllegalArgumentException.class,()->new ResultBundleContract().validate(decoded,context)).getMessage());
        assertEquals("exp_bad_schema",decoded.success().path("experiment_run_id").asText());
    }
    @Test void retentionDigestMustPreserveDuplicateMultiplicity() {
        assertEquals(List.of("a","a","b"),Qa04MlRecoveryAcceptance.canonicalRows(List.of("b","a","a")));
    }
}
