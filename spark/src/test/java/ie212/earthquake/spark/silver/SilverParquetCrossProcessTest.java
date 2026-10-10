package ie212.earthquake.spark.silver;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SilverParquetCrossProcessTest {
    @Test void bytesAreStableAcrossIndependentJvms() throws Exception {
        String baseline = null;
        for (int attempt = 0; attempt < 6; attempt++) {
            var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    Probe.class.getName(), Integer.toString(attempt)).redirectError(ProcessBuilder.Redirect.DISCARD).start();
            try {
                assertTrue(process.waitFor(60, TimeUnit.SECONDS), "bounded JVM probe must finish");
                assertEquals(0, process.exitValue());
                String hashes = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                if (baseline == null) { baseline = hashes; } else { assertEquals(baseline, hashes); }
            } finally { if (process.isAlive()) { process.destroyForcibly(); } }
        }
    }
    public static class Probe {
        public static void main(String[] args) throws Exception {
            // Spark/SDK initialization changes identity-hash allocation between submits.
            for (int i = 0; i < Integer.parseInt(args[0]) * 1000; i++) { System.identityHashCode(new Object()); }
            var serializer = new SilverParquetSerializer();
            System.out.println(SilverBundlePublisher.sha(serializer.serializeObservations(List.of())));
            System.out.println(SilverBundlePublisher.sha(serializer.serializeRejects(List.of())));
            System.out.println(SilverBundlePublisher.sha(serializer.serializeSourceLinks(List.of())));
            System.out.println(SilverBundlePublisher.sha(serializer.serializeCanonicalMemberships(List.of())));
            var time = java.time.Instant.parse("2026-10-10T00:00:00Z");
            System.out.println(SilverBundlePublisher.sha(serializer.serializeSourceLinks(List.of(
                    new SilverSourceLink("l", "u", "j", 1, 2, 3.0, 0.1, 0.9,
                            "ACCEPTED", List.of(), "v1", time)))));
            System.out.println(SilverBundlePublisher.sha(serializer.serializeCanonicalMemberships(List.of(
                    new SilverCanonicalMembership("c", "u", "PRIMARY", "l", "v1", time)))));
        }
    }
}
