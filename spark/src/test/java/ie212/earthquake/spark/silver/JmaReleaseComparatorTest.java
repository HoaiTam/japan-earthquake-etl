package ie212.earthquake.spark.silver;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmaReleaseComparatorTest {

    private final JmaReleaseComparator comparator = new JmaReleaseComparator();

    @Test
    void releaseV2IsGreaterThanReleaseV1() {
        assertTrue(comparator.compare("release-v2", "release-v1") > 0);
        assertTrue(comparator.compare("release-v1", "release-v2") < 0);
        assertEquals(0, comparator.compare("release-v1", "release-v1"));
    }

    @Test
    void handlesVersionNumberVariations() {
        assertTrue(comparator.compare("v10", "v2") > 0);
        assertTrue(comparator.compare("release-10", "release-2") > 0);
        assertTrue(comparator.compare("V2", "v1") > 0);
        assertEquals(0, comparator.compare("release-v1", "RELEASE-V1"));
    }

    @Test
    void comparesDottedVersionsNumerically() {
        assertTrue(comparator.compare("v2.1", "v2.0") > 0);
        assertTrue(comparator.compare("2.0.1", "2.0") > 0);
        assertTrue(comparator.compare("1.10", "1.2") > 0);
    }

    @Test
    void comparesTimestampedReleasesChronologically() {
        String older = "jma-lm-20160819T034858Z-sha256-116739000000";
        String newer = "jma-lm-20230901T120000Z-sha256-200000000000";
        assertTrue(comparator.compare(newer, older) > 0);
        assertTrue(comparator.compare(older, newer) < 0);
    }

    @Test
    void customInventoryOrderTakesPrecedence() {
        List<String> inventory = List.of("catalog-A", "catalog-C", "catalog-B");
        JmaReleaseComparator custom = new JmaReleaseComparator(inventory);

        // In inventory, catalog-C is at index 1 and catalog-B is at index 2
        assertTrue(custom.compare("catalog-B", "catalog-C") > 0);
        assertTrue(custom.compare("catalog-C", "catalog-A") > 0);
    }

    @Test
    void nullHandlingAndIdenticalValues() {
        assertTrue(comparator.compare("release-v1", null) > 0);
        assertTrue(comparator.compare(null, "release-v1") < 0);
        assertEquals(0, comparator.compare(null, null));
    }
}
