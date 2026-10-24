package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.util.*;
import org.apache.spark.sql.*;

/** DAT-03 owner decision: strict split, exclude incomplete windows, never clip or borrow a split. */
public final class WindowCoveragePolicy {
    public static final String STRICT = "dat03-strict-split-v1";
    public static final String LEGACY = "legacy-fail-export";
    private static final int MAX_EXCLUSIONS = 4096;
    private WindowCoveragePolicy() { }
    public record Decision(Dataset<Row> included, ObjectNode report) { }

    public static String fromManifest(JsonNode manifest) throws Exception {
        // Legacy component bundles may omit the runtime filter; no new policy is inferred.
        if(!manifest.has("filter_config_json")) return LEGACY;
        var filter=WindowArtifactInput.JSON.readTree(manifest.path("filter_config_json").asText());
        if(filter==null || !filter.isObject()) throw new IllegalArgumentException("DS_WINDOW_POLICY_UNSUPPORTED");
        if(!filter.has("window_coverage_policy")) return LEGACY;
        var policy=filter.path("window_coverage_policy");
        if(!policy.isTextual() || !STRICT.equals(policy.asText())) throw new IllegalArgumentException("DS_WINDOW_POLICY_UNSUPPORTED");
        return STRICT;
    }
    public static Decision apply(Dataset<Row> windows,MainshockSelectionConfig selection,String policy) {
        if(LEGACY.equals(policy)) return new Decision(windows,WindowArtifactInput.JSON.createObjectNode());
        if(!STRICT.equals(policy)) throw new IllegalArgumentException("DS_WINDOW_POLICY_UNSUPPORTED");
        var start=lit(Timestamp.from(selection.periodStartUtc())); var end=lit(Timestamp.from(selection.periodEndUtcExclusive()));
        if(windows.filter(col("window_start_utc").isNull().or(col("window_end_utc").isNull())
                .or(col("window_start_utc").gt(col("mainshock_time_utc")))
                .or(col("window_end_utc").lt(col("mainshock_time_utc")))).limit(1).count()!=0)
            throw new IllegalArgumentException("DS_WINDOW_DIMENSIONS_INVALID");
        // Window endpoints are inclusive, while the research split end is exclusive.
        var pre=col("window_start_utc").lt(start); var post=col("window_end_utc").geq(end);
        var excluded=windows.filter(pre.or(post)).withColumn("reason_code",
                when(pre.and(post),lit("PRE_AND_POST_OUTSIDE_SPLIT"))
                        .when(pre,lit("PRE_OUTSIDE_SPLIT")).otherwise(lit("POST_OUTSIDE_SPLIT")));
        var rows=excluded.select("mainshock_event_id","mainshock_time_utc","window_start_utc","window_end_utc","reason_code")
                .orderBy("mainshock_event_id").limit(MAX_EXCLUSIONS+1).collectAsList();
        if(rows.size()>MAX_EXCLUSIONS) throw new IllegalArgumentException("DS_WINDOW_EXCLUSION_REPORT_LIMIT");
        var included=windows.filter(not(pre.or(post)));
        var report=WindowArtifactInput.JSON.createObjectNode().put("policy_version",STRICT).put("dataset_split",selection.datasetSplit())
                .put("period_start_utc",selection.periodStartUtc().toString()).put("period_end_utc_exclusive",selection.periodEndUtcExclusive().toString())
                .put("eligible_mainshock_count",windows.count()).put("included_mainshock_count",included.count())
                .put("excluded_mainshock_count",rows.size()).put("windows_clipped",false).put("cross_split_data_used",false);
        var details=report.putArray("excluded_mainshocks"); var counts=new TreeMap<String,Integer>();
        for(var row:rows) {
            details.addObject().put("mainshock_event_id",row.getString(0)).put("event_time_utc",row.getTimestamp(1).toInstant().toString())
                    .put("required_start_utc",row.getTimestamp(2).toInstant().toString()).put("required_end_utc_inclusive",row.getTimestamp(3).toInstant().toString())
                    .put("reason_code",row.getString(4));
            counts.merge(row.getString(4),1,Integer::sum);
        }
        report.set("reason_counts",WindowArtifactInput.JSON.valueToTree(counts));
        validateReport(report,report.path("included_mainshock_count").asLong());
        return new Decision(included,report);
    }
    public static void validateReport(JsonNode report,long included) {
        var ids=new HashSet<String>(); var reasons=new TreeMap<String,Integer>();
        try {
            var start=java.time.Instant.parse(report.path("period_start_utc").asText());
            var end=java.time.Instant.parse(report.path("period_end_utc_exclusive").asText());
            if(!STRICT.equals(report.path("policy_version").asText()) || !start.isBefore(end)
                    || !report.path("windows_clipped").isBoolean() || report.path("windows_clipped").asBoolean()
                    || !report.path("cross_split_data_used").isBoolean() || report.path("cross_split_data_used").asBoolean()
                    || !report.path("excluded_mainshocks").isArray() || report.path("excluded_mainshocks").size()>MAX_EXCLUSIONS) throw new IllegalArgumentException();
            for(String k:List.of("eligible_mainshock_count","included_mainshock_count","excluded_mainshock_count"))
                if(!report.path(k).isIntegralNumber() || !report.path(k).canConvertToLong() || report.path(k).asLong()<0) throw new IllegalArgumentException();
            for(var item:report.path("excluded_mainshocks")) {
                if(!item.path("mainshock_event_id").isTextual() || item.path("mainshock_event_id").asText().isBlank()
                        || !ids.add(item.path("mainshock_event_id").asText())) throw new IllegalArgumentException();
                var t=java.time.Instant.parse(item.path("event_time_utc").asText());
                var a=java.time.Instant.parse(item.path("required_start_utc").asText()); var b=java.time.Instant.parse(item.path("required_end_utc_inclusive").asText());
                if(t.isBefore(start) || !t.isBefore(end) || a.isAfter(t) || b.isBefore(t)) throw new IllegalArgumentException();
                boolean pre=a.isBefore(start),post=!b.isBefore(end);
                String reason=pre&&post?"PRE_AND_POST_OUTSIDE_SPLIT":pre?"PRE_OUTSIDE_SPLIT":post?"POST_OUTSIDE_SPLIT":"";
                if(reason.isEmpty() || !reason.equals(item.path("reason_code").asText())) throw new IllegalArgumentException();
                reasons.merge(reason,1,Integer::sum);
            }
            if(included!=report.path("included_mainshock_count").asLong() || ids.size()!=report.path("excluded_mainshock_count").asLong()
                    || included!=report.path("eligible_mainshock_count").asLong()-ids.size()
                    || !DatasetIdentityPlan.canonical(WindowArtifactInput.JSON.valueToTree(reasons)).toString().equals(DatasetIdentityPlan.canonical(report.path("reason_counts")).toString()))
                throw new IllegalArgumentException();
        } catch(Exception error) { throw new IllegalArgumentException("DS_WINDOW_EXCLUSION_REPORT_INVALID"); }
    }
}
