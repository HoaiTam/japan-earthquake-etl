package ie212.earthquake.spark.gold;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** GLD-01 has no publication side effects; GLD-03/04 own commit and verification. */
public record GoldTransformationResult(Dataset<Row> eventCurrent, Dataset<Row> earthquakeEventCurrent,
        Dataset<Row> eventSourceBridge, Dataset<Row> dimDate, Dataset<Row> dimRegion,
        Dataset<Row> dimMagnitudeBand, Dataset<Row> dimDepthBand,
        long currentObservationCount, long canonicalEventCount, long bridgeRowCount) { }
