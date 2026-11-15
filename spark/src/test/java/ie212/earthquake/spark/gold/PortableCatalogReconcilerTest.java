package ie212.earthquake.spark.gold;

import java.nio.file.Files;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PortableCatalogReconcilerTest {
    @Test void globalBoundaryRevisionLinkAndExactDuplicateFixtures() throws Exception {
        var spark = SparkSession.builder().master("local[1]").appName("portable-import-fixtures")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try { assertEquals(14, PortableImporterFixtureJob.runAll(spark, Files.createTempDirectory("portable-import-fixtures").toRealPath())); }
        finally { spark.stop(); }
    }
}
