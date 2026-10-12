package ie212.earthquake.spark.ml;

import ie212.earthquake.spark.silver.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JmaMainshockPilotSamplerTest {
    private byte[] row(int index, String magnitude, String depth) throws Exception {
        String fixture;
        Path root = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (!Files.exists(root.resolve("tests/fixtures/jma/fixed-width/success.hyp"))) {
            root = Objects.requireNonNull(root.getParent(), "repository fixture root");
        }
        try (var input = Files.newInputStream(root.resolve("tests/fixtures/jma/fixed-width/success.hyp"))) {
            fixture = new String(input.readNBytes(96), StandardCharsets.ISO_8859_1);
        }
        String time = LocalDateTime.of(2023, 1, 14, 0, 0).plusHours(index).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + "00";
        return new StringBuilder(fixture).replace(1, 17, time).replace(44, 49, depth).replace(52, 54, magnitude)
            .toString().getBytes(StandardCharsets.ISO_8859_1);
    }
    private byte[] archive(boolean mainshock) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("h2023"));
            for (int i = 0; i < 64; i++) {
                zip.write(row(i, mainshock && i == 31 ? "60" : "20", mainshock && i == 31 ? "10000" : "01000"));
                zip.write('\n');
            }
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
    private JmaParseContext context(byte[] archive) {
        return new JmaParseContext("fixture-bronze", "s3://fixture/archive.zip", SourceKeyGenerator.sha256(archive), "fixture-ingest",
            "fixture-release", null, "https://www.data.jma.go.jp/archive.zip", "h2023", Instant.parse("2026-10-12T00:00:00Z"));
    }
    @Test void samplePreservesPhysicalLineageAndIsDeterministic() throws Exception {
        byte[] archive = archive(true); var context = context(archive);
        var first = JmaMainshockPilotSampler.select(archive, context, 2023);
        assertEquals(first, JmaMainshockPilotSampler.select(archive, context, 2023));
        assertEquals(64, first.physicalCount()); assertEquals(64, first.observations().size());
        assertEquals("member=h2023;line=32", first.centre().rawRecordLocator());
        assertEquals(6.0, first.centre().magnitude()); assertEquals(100.0, first.centre().depthKm());
        assertEquals(Duration.ofDays(14), Duration.between(first.start(), first.end()));
        assertEquals(64, first.observations().stream().map(SilverObservation::sourceObservationId).distinct().count());
    }
    @Test void missingMainshockAndWrongArchivePinsFailClosed() throws Exception {
        byte[] archive = archive(false); var context = context(archive);
        assertEquals("ML_PILOT_MAINSHOCK_NOT_FOUND", assertThrows(IOException.class,
            () -> JmaMainshockPilotSampler.select(archive, context, 2023)).getMessage());
        assertThrows(IOException.class, () -> JmaMainshockPilotSampler.select(archive(true), context, 2023));
        assertThrows(IOException.class, () -> JmaMainshockPilotSampler.select(archive, context, 2022));
    }
    @Test void strictMagnitudeAndInclusiveDepthBoundariesRemainUnchanged() throws Exception {
        var parser = new JmaFixedWidthParser(); var context = context(archive(true));
        assertFalse(JmaMainshockPilotSampler.mainshock(parser.parseVerifiedRecord(row(0, "55", "10000"), 1, context).observations().get(0), 2023));
        assertTrue(JmaMainshockPilotSampler.mainshock(parser.parseVerifiedRecord(row(0, "56", "05000"), 1, context).observations().get(0), 2023));
        assertTrue(JmaMainshockPilotSampler.mainshock(parser.parseVerifiedRecord(row(0, "56", "20000"), 1, context).observations().get(0), 2023));
        assertFalse(JmaMainshockPilotSampler.mainshock(parser.parseVerifiedRecord(row(0, "56", "20001"), 1, context).observations().get(0), 2023));
    }
    @Test void singleRecordApiRejectsWrongWidthAndRetainsRejectLocator() throws Exception {
        var parser = new JmaFixedWidthParser(); var context = context(archive(true));
        var unknownUrl = new JmaParseContext(context.bronzeManifestId(), context.rawObjectUri(), context.rawSha256(), context.ingestRunId(),
            context.catalogRelease(), context.catalogReleaseAtUtc(), null, context.memberName(), context.processedAtUtc());
        assertNull(parser.parseVerifiedRecord(row(0, "20", "01000"), 10, unknownUrl).observations().get(0).sourceUrl());
        byte[] validTime = row(0, "20", "01000");
        assertEquals(parser.parseVerifiedRecord(validTime, 10, unknownUrl).observations().get(0).eventTimeUtc(),
            JmaFixedWidthParser.originTimeOfVerifiedRecord(validTime));
        byte[] badTime = validTime.clone(); badTime[5] = '9';
        assertNull(JmaFixedWidthParser.originTimeOfVerifiedRecord(badTime));
        assertThrows(IOException.class, () -> parser.parseVerifiedRecord(new byte[95], 10, context));
        assertThrows(IllegalArgumentException.class, () -> parser.parseVerifiedRecord(row(0, "20", "01000"), 0, context));
        byte[] invalid = row(0, "20", "01000"); invalid[0] = '?';
        assertEquals("member=h2023;line=901", parser.parseVerifiedRecord(invalid, 901, context).rejects().get(0).rawRecordLocator());
    }
}
