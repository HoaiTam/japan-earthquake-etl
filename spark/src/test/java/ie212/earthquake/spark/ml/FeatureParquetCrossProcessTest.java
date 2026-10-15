package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

class FeatureParquetCrossProcessTest {
    @org.junit.jupiter.api.io.TempDir Path temporary;
    @Test void sparkParquetBytesAreStableAcrossIndependentJvms() throws Exception {
        String baseline = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                Probe.class.getName(), temporary.resolve("part-" + attempt + ".parquet").toString(), Integer.toString(attempt))
                .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            try {
                assertTrue(process.waitFor(45, TimeUnit.SECONDS), "bounded Spark probe must finish");
                assertEquals(0, process.exitValue());
                String sha = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
                assertTrue(sha.matches("[a-f0-9]{64}"));
                if (baseline == null) baseline = sha; else assertEquals(baseline, sha);
            } finally { if (process.isAlive()) process.destroyForcibly(); }
        }
    }
    public static class Probe {
        public static void main(String[] args) throws Exception {
            for (int i = 0; i < Integer.parseInt(args[1]) * 1000; i++) System.identityHashCode(new Object());
            var spark = LocalFeatureJob.session();
            try {
                var schema = new StructType().add("id", "string").add("x_scaled", "double").add("is_mainshock", "boolean").add("candidate_time_utc", "timestamp");
                var time = Timestamp.from(Instant.parse("2000-01-01T00:00:00Z"));
                var rows = spark.createDataFrame(List.of(RowFactory.create("a", 0., true, time), RowFactory.create("b", 1., false, time)), schema);
                LocalFeatureJob.parquet(rows, Path.of(args[0]), "id");
                System.out.println(WindowArtifactInput.sha(Path.of(args[0])));
            } finally { spark.stop(); }
        }
    }
}
