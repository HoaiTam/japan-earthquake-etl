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
        File out = outputDir == null || outputDir.isBlank() ? null : new File(outputDir);
        if (out != null) {
            Files.createDirectories(out.toPath());
            // Invalidate before computation too: a validation failure on rerun is not success.
            Files.deleteIfExists(new File(out, "window_audit_report.json").toPath());
            Files.deleteIfExists(new File(out, "window_audit_summary.md").toPath());
        }
        MainshockWindowEngine engine = new MainshockWindowEngine();
        MainshockWindowResult result = engine.execute(
                auditedEvents, selectionConfig, windowConfig, mcValue, datasetId, createdAtUtc
        );

        // Verify all invariants programmatically
        result.assertInvariants();

        if (out != null) {
            File jsonReport = new File(out, "window_audit_report.json");
            File mdSummary = new File(out, "window_audit_summary.md");
            // A stale success report must not survive a failed rerun. Snapshots are not a
            // multi-table atomic publication: consumers wait for the completed audit report.
            if (result.rejectedWindowCount() > 0) {
                throw new IllegalStateException("ML_RESOURCE_LIMIT_EXCEEDED: rejected windows cannot be exported");
            }
            File mainshockParquet = new File(out, "mainshock_candidate_snapshot.parquet");
            result.mainshockSnapshot().write().mode(SaveMode.Overwrite).parquet(mainshockParquet.getAbsolutePath());
            File candidateParquet = new File(out, "sequence_candidate_snapshot.parquet");
            result.candidateSnapshot().write().mode(SaveMode.Overwrite).parquet(candidateParquet.getAbsolutePath());
            // Propagate any storage/configuration failure; no implicit JSON fallback.
            if (auditedEvents.sparkSession().read().parquet(mainshockParquet.getAbsolutePath()).count() != result.mainshockCount()
                    || auditedEvents.sparkSession().read().parquet(candidateParquet.getAbsolutePath()).count() != result.totalCandidateRowCount()) {
                throw new IOException("Parquet readback count mismatch");
            }
            Files.writeString(mdSummary.toPath(), result.toMarkdownSummary(), StandardCharsets.UTF_8);
            Files.writeString(jsonReport.toPath(), result.toJsonReport(), StandardCharsets.UTF_8);
        }

        return result;
    }
}
