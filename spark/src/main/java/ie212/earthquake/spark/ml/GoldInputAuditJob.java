package ie212.earthquake.spark.ml;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Entry point for running Gold Input Audit and Completeness Evaluation.
 */
public final class GoldInputAuditJob {

    public static GoldAuditResult runAudit(
            SparkSession spark,
            Dataset<Row> goldEvents,
            GoldAuditConfig config,
            Instant auditedAtUtc
    ) {
        Objects.requireNonNull(spark, "spark");
        Objects.requireNonNull(goldEvents, "goldEvents");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(auditedAtUtc, "auditedAtUtc");

        GoldInputAuditEngine engine = new GoldInputAuditEngine();
        return engine.audit(goldEvents, config, auditedAtUtc);
    }

    public static void writeReports(GoldAuditResult result, Path reportJsonPath, Path reportMdPath) {
        try {
            if (reportJsonPath != null) {
                if (reportJsonPath.getParent() != null) {
                    Files.createDirectories(reportJsonPath.getParent());
                }
                Files.writeString(reportJsonPath, result.toJsonReport().toPrettyString());
            }
            if (reportMdPath != null) {
                if (reportMdPath.getParent() != null) {
                    Files.createDirectories(reportMdPath.getParent());
                }
                Files.writeString(reportMdPath, result.toMarkdownReport());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write audit reports", e);
        }
    }
}
