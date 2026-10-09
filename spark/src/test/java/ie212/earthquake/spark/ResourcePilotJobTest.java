package ie212.earthquake.spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResourcePilotJobTest {
    @Test void selectsOnlyExactBoundedLines() throws Exception {
        byte[] input = ("a".repeat(96) + "\n" + "b".repeat(96) + "\n").getBytes(StandardCharsets.US_ASCII);
        assertEquals("a".repeat(96) + "\n", new String(ResourcePilotJob.firstLines(
                new ByteArrayInputStream(input), 1), StandardCharsets.US_ASCII));
    }
    @Test void normalizesFinalLineAndAcceptsCrlf() throws Exception {
        for (String input : new String[] {"a".repeat(96), "a".repeat(96) + "\r\n"}) {
            assertTrue(new String(ResourcePilotJob.firstLines(new ByteArrayInputStream(
                    input.getBytes(StandardCharsets.US_ASCII)), 1), StandardCharsets.US_ASCII).endsWith("\n"));
        }
    }
    @Test void rejectsUnboundedLinesAndBadLimit() {
        assertThrows(IOException.class, () -> ResourcePilotJob.firstLines(new ByteArrayInputStream(
                "a".repeat(98).getBytes(StandardCharsets.US_ASCII)), 1));
        assertThrows(IOException.class, () -> ResourcePilotJob.firstLines(new ByteArrayInputStream(new byte[0]), 10001));
    }
}
