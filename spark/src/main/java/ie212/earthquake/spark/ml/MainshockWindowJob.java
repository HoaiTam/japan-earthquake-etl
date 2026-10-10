package ie212.earthquake.spark.ml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Entry point job for mainshock selection and candidate window generation (MLD-03).
 */
public final class MainshockWindowJob {

    public static MainshockWindowResult run(
            Dataset<Row> auditedEvents,
            MainshockSelectionConfig selectionConfig,
            WindowModelConfig windowConfig,
            double mcValue,
            String datasetId,
            Instant createdAtUtc,
            String outputDir
    ) throws IOException {
        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selectionConfig, windowConfig, mcValue, datasetId, createdAtUtc
        );

        // Verify all invariants programmatically
        result.assertInvariants();

        if (outputDir != null && !outputDir.isBlank()) {
            File out = new File(outputDir);
            out.mkdirs();

            File jsonReport = new File(out, "window_audit_report.json");
            Files.writeString(jsonReport.toPath(), result.toJsonReport(), StandardCharsets.UTF_8);

            File mdSummary = new File(out, "window_audit_summary.md");
            Files.writeString(mdSummary.toPath(), result.toMarkdownSummary(), StandardCharsets.UTF_8);

            try {
                File mainshockParquet = new File(out, "mainshock_candidate_snapshot.parquet");
                result.mainshockSnapshot().write().mode(SaveMode.Overwrite).parquet(mainshockParquet.getAbsolutePath());

                File candidateParquet = new File(out, "sequence_candidate_snapshot.parquet");
                result.candidateSnapshot().write().mode(SaveMode.Overwrite).parquet(candidateParquet.getAbsolutePath());
            } catch (Throwable t) {
                // Fallback for Windows local test environments without winutils.exe:
                // Export snapshots as JSON serialization to maintain verification artifacts
                File mainshockJson = new File(out, "mainshock_candidate_snapshot.json");
                Files.writeString(mainshockJson.toPath(), result.mainshockSnapshot().toJSON().collectAsList().toString(), StandardCharsets.UTF_8);

                File candidateJson = new File(out, "sequence_candidate_snapshot.json");
                Files.writeString(candidateJson.toPath(), result.candidateSnapshot().toJSON().collectAsList().toString(), StandardCharsets.UTF_8);
            }
        }

        return result;
    }
}
