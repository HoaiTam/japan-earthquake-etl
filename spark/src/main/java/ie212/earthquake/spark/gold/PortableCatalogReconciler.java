package ie212.earthquake.spark.gold;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.*;
import org.apache.spark.sql.*;

/** Global/boundary gates over ALL prepared rows, not a sample or per-batch-only proof. */
public final class PortableCatalogReconciler {
    public record Result(Dataset<Row> events, Dataset<Row> bridge, Dataset<Row> links, ObjectNode report) implements AutoCloseable {
        @Override public void close() { events.unpersist(); bridge.unpersist(); links.unpersist(); }
    }
    private PortableCatalogReconciler() { }
    private static void empty(Dataset<Row> rows, String reason) throws IOException {
        if (rows.limit(1).count() != 0) throw new IOException(reason);
    }
    private static Column blank(String name) { return col(name).isNull().or(length(trim(col(name))).equalTo(0)); }
    public static Result reconcile(Dataset<Row> inputEvents, Dataset<Row> inputBridge, Dataset<Row> inputLinks,
            long expectedEvents, long expectedBridge, long expectedLinks) throws Exception {
        long rawEvents = inputEvents.count(), rawBridge = inputBridge.count(), rawLinks = inputLinks.count();
        PortableImportContract.require(rawEvents == expectedEvents && rawBridge == expectedBridge && rawLinks == expectedLinks,
                "GLOBAL_INPUT_COUNT_PIN_MISMATCH");
        Dataset<Row> events = inputEvents.dropDuplicates().persist(org.apache.spark.storage.StorageLevel.DISK_ONLY());
        Dataset<Row> bridge = inputBridge.dropDuplicates().persist(org.apache.spark.storage.StorageLevel.DISK_ONLY());
        Dataset<Row> links = inputLinks.dropDuplicates().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
        try {
        long eventCount = events.count(), bridgeCount = bridge.count(), linkCount = links.count();
        empty(events.filter(blank("canonical_event_id").or(blank("primary_observation_id")).or(blank("canonical_model_version"))
                .or(col("event_time_utc").isNull()).or(col("source_count").isNull()).or(col("has_usgs").isNull()).or(col("has_jma").isNull())
                .or(col("quality_status").isNull()).or(not(col("quality_status").isin("VALID", "WARNING")))
                .or(col("event_time_utc").lt(Timestamp.from(Instant.parse("2000-01-01T00:00:00Z"))))
                .or(col("event_time_utc").geq(Timestamp.from(Instant.parse("2024-01-01T00:00:00Z"))))), "INVALID_GLOBAL_EVENT");
        empty(events.groupBy("canonical_event_id").count().filter(col("count").gt(1)), "BOUNDARY_CANONICAL_CONFLICT");
        PortableImportContract.require(events.select("canonical_model_version").distinct().count() <= 1,
                "CANONICAL_MODEL_VERSION_CONFLICT");
        empty(bridge.filter(blank("canonical_event_id").or(blank("source_observation_id")).or(blank("source_system"))
                .or(blank("source_record_key")).or(blank("bronze_manifest_id")).or(blank("raw_object_uri"))
                .or(col("is_primary").isNull()).or(not(col("source_system").isin("USGS", "JMA_BULLETIN")))), "INVALID_GLOBAL_BRIDGE");
        empty(bridge.groupBy("source_observation_id").count().filter(col("count").gt(1)), "BOUNDARY_OBSERVATION_CONFLICT");
        // Imported Silver already selected revisions. Do not choose a winner using incomplete Gold fields.
        // Conflicting current revisions must return to the source resolver with their original evidence.
        empty(bridge.groupBy("source_system", "source_record_key").count().filter(col("count").gt(1)),
                "GLOBAL_REVISION_CONFLICT_REQUIRES_SOURCE_RESOLUTION");
        empty(bridge.join(events.select("canonical_event_id"), "canonical_event_id", "left_anti"), "ORPHAN_GLOBAL_BRIDGE");
        empty(events.join(bridge.select("canonical_event_id").distinct(), "canonical_event_id", "left_anti"), "EVENT_WITHOUT_BRIDGE");
        empty(bridge.groupBy("canonical_event_id", "source_system").count().filter(col("count").gt(1)), "MULTIPLE_CURRENT_MEMBERS_FROM_ONE_SOURCE");
        Dataset<Row> summary = bridge.groupBy("canonical_event_id").agg(count(lit(1)).alias("actual_sources"),
                sum(when(col("is_primary"), 1).otherwise(0)).alias("actual_primary"),
                max(when(col("source_system").equalTo("USGS"), 1).otherwise(0)).alias("actual_usgs"),
                max(when(col("source_system").equalTo("JMA_BULLETIN"), 1).otherwise(0)).alias("actual_jma"));
        empty(events.join(summary, "canonical_event_id").filter(col("actual_primary").notEqual(1)
                .or(col("source_count").notEqual(col("actual_sources")))
                .or(not(col("has_usgs").eqNullSafe(col("actual_usgs").gt(0))))
                .or(not(col("has_jma").eqNullSafe(col("actual_jma").gt(0))))), "GLOBAL_PRIMARY_OR_SOURCE_COUNT_UNBALANCED");
        Dataset<Row> primary = bridge.filter(col("is_primary")).select(col("canonical_event_id"), col("source_observation_id").alias("actual_primary_id"));
        empty(events.join(primary, "canonical_event_id").filter(not(col("primary_observation_id").eqNullSafe(col("actual_primary_id")))), "GLOBAL_PRIMARY_ID_MISMATCH");
        empty(links.filter(blank("source_link_id").or(blank("left_observation_id")).or(blank("right_observation_id"))
                .or(col("link_decision").isNull()).or(not(col("link_decision").isin("ACCEPTED", "REJECTED", "AMBIGUOUS")))), "INVALID_GLOBAL_SOURCE_LINK");
        empty(links.groupBy("source_link_id").count().filter(col("count").gt(1)), "BOUNDARY_SOURCE_LINK_CONFLICT");
        Dataset<Row> ids = bridge.select("source_observation_id");
        empty(links.select(col("left_observation_id").alias("source_observation_id"))
                .union(links.select(col("right_observation_id").alias("source_observation_id")))
                .join(ids, "source_observation_id", "left_anti"), "GLOBAL_LINK_ENDPOINT_MISSING");
        Dataset<Row> paired = links.join(bridge.select(col("source_observation_id").alias("left_observation_id"),
                col("canonical_event_id").alias("left_canonical")), "left_observation_id")
                .join(bridge.select(col("source_observation_id").alias("right_observation_id"),
                col("canonical_event_id").alias("right_canonical")), "right_observation_id");
        empty(paired.filter(col("link_decision").equalTo("ACCEPTED").and(not(col("left_canonical").eqNullSafe(col("right_canonical"))))
                .or(not(col("link_decision").equalTo("ACCEPTED")).and(col("left_canonical").eqNullSafe(col("right_canonical"))))),
                "GLOBAL_LINK_DECISION_MEMBERSHIP_CONFLICT");
        Dataset<Row> accepted = paired.filter(col("link_decision").equalTo("ACCEPTED"));
        empty(bridge.filter(col("source_link_id").isNotNull()).join(accepted.select("source_link_id"), "source_link_id", "left_anti"),
                "BRIDGE_REFERS_TO_NON_ACCEPTED_LINK");
        empty(bridge.filter(not(col("is_primary"))).filter(col("source_link_id").isNull()), "SUPPORTING_MEMBER_WITHOUT_ACCEPTED_LINK");
        empty(bridge.filter(col("source_link_id").isNotNull()).join(accepted.select("source_link_id", "left_observation_id", "right_observation_id"), "source_link_id")
                .filter(not(col("source_observation_id").equalTo(col("left_observation_id")).or(col("source_observation_id").equalTo(col("right_observation_id"))))), "BRIDGE_LINK_ENDPOINT_MISMATCH");
        long acceptedCount = accepted.count();
        PortableImportContract.require(acceptedCount == bridgeCount - eventCount, "GLOBAL_ACCEPTED_LINK_RECONCILIATION_FAILED");
        ObjectNode report = PortableImportContract.JSON.createObjectNode().put("version", "dat03-global-reconciliation-v1");
        var counts = report.putObject("counts").put("input_events", rawEvents).put("input_bridge", rawBridge).put("input_links", rawLinks)
                .put("exact_event_duplicates", rawEvents - eventCount).put("exact_bridge_duplicates", rawBridge - bridgeCount)
                .put("exact_link_duplicates", rawLinks - linkCount).put("canonical_events", eventCount).put("bridge_rows", bridgeCount)
                .put("source_links", linkCount).put("accepted_source_links", acceptedCount);
        report.put("balanced", true).put("global_checks_passed", true).put("boundary_checks_passed", true)
                .put("conflicting_revision_policy", "FAIL_CLOSED_REQUIRES_SOURCE_RESOLUTION")
                .put("gold_published", false).put("official_export_ready", false);
        return new Result(events, bridge, links, report);
        } catch (Exception failure) { events.unpersist(); bridge.unpersist(); links.unpersist(); throw failure; }
    }
}
