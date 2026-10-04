package ie212.earthquake.spark;

import java.util.Locale;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

/**
 * Minimal distributed job used to verify the SPK-01 standalone cluster.
 */
public final class HelloWorldJob {
    static final long EXPECTED_RECORD_COUNT = 10L;
    static final long EXPECTED_ID_SUM = 45L;

    private HelloWorldJob() {
    }

    /**
     * Starts a Spark application and validates a deterministic distributed aggregation.
     *
     * @param args unused command-line arguments
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("spk-01-hello-world")
                .getOrCreate();

        try {
            Row summary = spark.range(0L, EXPECTED_RECORD_COUNT, 1L, 2)
                    .agg(
                            count("*").alias("record_count"),
                            sum("id").alias("id_sum"))
                    .first();

            long recordCount = summary.getLong(0);
            long idSum = summary.getLong(1);
            validateSummary(recordCount, idSum);

            System.out.println(formatSuccessEvent(
                    spark.sparkContext().applicationId(),
                    spark.sparkContext().master(),
                    recordCount,
                    idSum));
        } finally {
            spark.stop();
        }
    }

    static void validateSummary(long recordCount, long idSum) {
        if (recordCount != EXPECTED_RECORD_COUNT || idSum != EXPECTED_ID_SUM) {
            throw new IllegalStateException(String.format(
                    Locale.ROOT,
                    "Unexpected Spark summary: record_count=%d id_sum=%d",
                    recordCount,
                    idSum));
        }
    }

    static String formatSuccessEvent(
            String applicationId,
            String master,
            long recordCount,
            long idSum) {
        return String.format(
                Locale.ROOT,
                "event=spark_hello_world_success app_id=%s master=%s record_count=%d id_sum=%d",
                applicationId,
                master,
                recordCount,
                idSum);
    }
}
