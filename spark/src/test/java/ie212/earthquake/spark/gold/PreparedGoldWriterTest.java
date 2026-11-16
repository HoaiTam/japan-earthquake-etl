package ie212.earthquake.spark.gold;

import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PreparedGoldWriterTest {
    @TempDir Path root;
    @Test void actualLocalIcebergPreparedAndLegacyCommitRerunAndConflictGuards() throws Exception {
        var spark = SparkSession.builder().master("local[1]").appName("prepared-gold-iceberg-regression")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false").config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "hadoop").config("spark.sql.catalog.iceberg.warehouse", root.resolve("warehouse").toUri().toString()).getOrCreate();
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl", GoldIcebergWriterTest.NioTestFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache", true);
        }
        spark.sparkContext().setLogLevel("ERROR");
        try { assertEquals(10, PreparedGoldWriterFixtureJob.runAll(spark, root)); }
        finally { spark.stop(); }
    }
}
