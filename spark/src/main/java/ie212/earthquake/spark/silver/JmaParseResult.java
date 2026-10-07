package ie212.earthquake.spark.silver;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** One archive/member result; duplicates and older revisions are deliberately retained. */
public record JmaParseResult(
        List<SilverObservation> observations,
        List<SilverRejectRecord> rejects,
        List<JmaNativeFields> nativeFields) {

    public JmaParseResult {
        observations = List.copyOf(observations);
        rejects = List.copyOf(rejects);
        nativeFields = List.copyOf(nativeFields);
        if (nativeFields.size() != observations.size()) {
            throw new IllegalArgumentException("each successful observation requires native audit fields");
        }
        for (int index = 0; index < observations.size(); index++) {
            if (!observations.get(index).rawRecordLocator().equals(nativeFields.get(index).rawRecordLocator())) {
                throw new IllegalArgumentException("native fields must match observation locators in order");
            }
        }
    }

    public int parsedCount() { return observations.size() + rejects.size(); }
    public int validCount() { return observations.size(); }
    public int rejectedCount() { return rejects.size(); }

    public Dataset<Row> observationDataset(SparkSession spark) {
        requireUtc(spark);
        return SilverSchemas.toObservationDataset(spark, observations);
    }

    public Dataset<Row> rejectDataset(SparkSession spark) {
        requireUtc(spark);
        return SilverSchemas.toRejectDataset(spark, rejects);
    }

    private static void requireUtc(SparkSession spark) {
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) {
            throw new IllegalArgumentException("Spark session timezone must be UTC for CON-03");
        }
    }
}
