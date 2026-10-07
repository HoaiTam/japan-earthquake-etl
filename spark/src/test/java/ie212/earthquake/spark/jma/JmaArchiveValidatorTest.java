package ie212.earthquake.spark.jma;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JmaArchiveValidatorTest {
    static final Path FIXTURES = Path.of("../tests/fixtures/jma/archives");
    private final JmaArchiveValidator validator = new JmaArchiveValidator();

    @Test
    void sharedFixturesRemainRawIncludingDuplicatesAndOutOfYearRecords() throws Exception {
        for (String name : new String[] {"success", "duplicate", "revision-v1", "revision-v2", "timezone-boundary", "ambiguous"}) {
            var result = validator.validate(Files.readAllBytes(FIXTURES.resolve(name + ".zip")), "hypo.dat");
            assertTrue(result.valid(), name + ": " + result.reason());
            assertTrue(result.recordCount() > 0);
        }
        var empty = validator.validate(Files.readAllBytes(FIXTURES.resolve("empty.zip")), "hypo.dat");
        assertTrue(empty.valid());
        assertEquals(0L, empty.recordCount());
        assertFalse(validator.validate(Files.readAllBytes(FIXTURES.resolve("invalid-record-length.zip")), "hypo.dat").valid());
    }

    @Test
    void countsBytesNotUnicodeAndAcceptsLfCrLfAndFinalUnterminatedRecord() throws Exception {
        byte[] record = new byte[96];
        Arrays.fill(record, (byte) 0x81); // Deliberately not valid UTF-8; byte width is the contract.
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        content.write(record);
        content.write('\n');
        content.write(record);
        content.write('\r');
        content.write('\n');
        content.write(record);
        var result = validator.validate(zip("h2023", content.toByteArray()), "h2023");
        assertTrue(result.valid());
        assertEquals(3L, result.recordCount());
    }

    @Test
    void rejectsWrongMissingExtraAndUnsafeMembers() throws Exception {
        byte[] row = "J".repeat(96).getBytes(StandardCharsets.US_ASCII);
        for (String name : new String[] {"wrong", "../h2023", "/h2023", "dir/h2023", "dir\\h2023", "h2023/"}) {
            assertFalse(validator.validate(zip(name, row), "h2023").valid(), name);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            for (String name : new String[] {"h2023", "extra"}) {
                zip.putNextEntry(new ZipEntry(name));
                zip.write(row);
                zip.closeEntry();
            }
        }
        assertEquals("UNEXPECTED_MEMBER_COUNT", validator.validate(bytes.toByteArray(), "h2023").reason());
        assertThrows(IllegalArgumentException.class, () -> validator.validate(bytes.toByteArray(), "../h2023"));
        bytes.reset();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            // An empty member is valid, but a container with no member is not.
        }
        assertEquals("UNEXPECTED_MEMBER_COUNT", validator.validate(bytes.toByteArray(), "h2023").reason());
    }

    @Test
    void rejectsHtmlTruncatedZipAndCorruptedStoredMemberCrc() throws Exception {
        assertEquals("INVALID_ZIP", validator.validate("<html>error</html>".getBytes(StandardCharsets.UTF_8), "h2023").reason());
        byte[] archive = zip("h2023", "J".repeat(96).getBytes(StandardCharsets.US_ASCII));
        assertFalse(validator.validate(Arrays.copyOf(archive, archive.length - 22), "h2023").valid());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] row = "J".repeat(96).getBytes(StandardCharsets.US_ASCII);
        CRC32 crc = new CRC32();
        crc.update(row);
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            ZipEntry entry = new ZipEntry("h2023");
            entry.setMethod(ZipEntry.STORED);
            entry.setSize(row.length);
            entry.setCrc(crc.getValue());
            zip.putNextEntry(entry);
            zip.write(row);
            zip.closeEntry();
        }
        byte[] corrupt = bytes.toByteArray();
        corrupt[30 + "h2023".length()] ^= 1; // Local-header data starts after name, no extra field.
        assertEquals("ZIP_CRC_OR_SIZE_MISMATCH", validator.validate(corrupt, "h2023").reason());
    }

    @Test
    void guardsCompressedAndUncompressedSizesAndRejectsBadLineEndings() throws Exception {
        byte[] archive = zip("h2023", "J".repeat(96).getBytes(StandardCharsets.US_ASCII));
        assertEquals("ARCHIVE_SIZE_LIMIT", new JmaArchiveValidator(10, 100).validate(archive, "h2023").reason());
        assertEquals("MEMBER_SIZE_LIMIT", new JmaArchiveValidator(1024, 95).validate(archive, "h2023").reason());
        for (String text : new String[] {"J".repeat(95), "J".repeat(97), "\n", "J".repeat(96) + "\r", "J".repeat(96) + "\r\r\n", "J".repeat(96) + "\n\n"}) {
            assertEquals("INVALID_RECORD_LENGTH", validator.validate(zip("h2023", text.getBytes(StandardCharsets.US_ASCII)), "h2023").reason());
        }
        assertThrows(IllegalArgumentException.class, () -> new JmaArchiveValidator(0, 100));
        assertThrows(IllegalArgumentException.class, () -> new JmaArchiveValidator(100, -1));
    }

    static byte[] zip(String member, byte[] content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry(member));
            zip.write(content);
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
}
