package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class ResearchVerificationBenchmarkTest {
    private final ObjectMapper json=new ObjectMapper();
    @Test void requiresExactNonnegativeIntegralScalar() throws Exception {
        assertEquals(7,ResearchVerificationBenchmark.scalar(new GoldPublisher.Result(List.of(),List.of(json.readTree("[7]")))));
        for(String value:List.of("[true]","[0.0]","[-1]","[null]","[]","[0,0]","[9223372036854775808]"))
            assertThrows(IllegalArgumentException.class,()->ResearchVerificationBenchmark.scalar(new GoldPublisher.Result(List.of(),List.of(json.readTree(value)))));
        assertThrows(IllegalArgumentException.class,()->ResearchVerificationBenchmark.scalar(new GoldPublisher.Result(List.of(),List.of())));
    }
    @Test void rejectsUnknownCheckBeforeAnyRuntimeAccess() {
        assertThrows(IllegalArgumentException.class,()->ResearchVerificationBenchmark.main(new String[]{"absent","arbitrary-sql","out"}));
    }
}
