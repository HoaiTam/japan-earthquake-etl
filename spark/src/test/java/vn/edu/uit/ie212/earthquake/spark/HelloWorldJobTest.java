package vn.edu.uit.ie212.earthquake.spark;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HelloWorldJobTest {
    @Test
    void acceptsExpectedDistributedSummary() {
        assertDoesNotThrow(() -> HelloWorldJob.validateSummary(10L, 45L));
    }

    @Test
    void rejectsUnexpectedDistributedSummary() {
        assertThrows(
                IllegalStateException.class,
                () -> HelloWorldJob.validateSummary(9L, 36L));
    }

    @Test
    void emitsStructuredSuccessEvent() {
        assertEquals(
                "event=spark_hello_world_success app_id=app-001 "
                        + "master=spark://spark-master:7077 record_count=10 id_sum=45",
                HelloWorldJob.formatSuccessEvent(
                        "app-001",
                        "spark://spark-master:7077",
                        10L,
                        45L));
    }
}
