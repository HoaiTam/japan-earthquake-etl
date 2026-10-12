package ie212.earthquake.spark.ml;

import ie212.earthquake.spark.silver.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.zip.ZipInputStream;

/** QA only: deterministic mainshock-centred fourteen-day slice; never production catalog coverage. */
public final class JmaMainshockPilotSampler {
    public static final int MAX_SELECTED = 15_000;
    public static final int MAX_PHYSICAL_LINES = 400_000;
    public static final String VERSION = "first-mainshock-centred-14d-v1";
    public record Sample(SilverObservation centre, List<SilverObservation> observations, List<SilverRejectRecord> rejects,
                         int physicalCount, Instant start, Instant end) { }
    private JmaMainshockPilotSampler() { }
    public static boolean mainshock(SilverObservation observation, int year) {
        Instant lower = LocalDate.of(year, 1, 8).atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant upper = LocalDate.of(year, 12, 24).atStartOfDay(ZoneOffset.UTC).toInstant();
        return "J".equals(observation.determiningAgencyCode()) && "EARTHQUAKE".equals(observation.eventTypeCode())
            && observation.isInStudyArea() && "UNIFIED".equals(observation.catalogEra())
            && observation.depthKm() != null && Double.isFinite(observation.depthKm())
            && observation.depthKm() >= 50 && observation.depthKm() <= 200
            && observation.magnitude() != null && Double.isFinite(observation.magnitude()) && observation.magnitude() > 5.5
            && !observation.eventTimeUtc().isBefore(lower) && observation.eventTimeUtc().isBefore(upper);
    }
    public static Sample select(byte[] verifiedArchive, JmaParseContext context, int year) throws IOException {
        if (verifiedArchive.length > 32 * 1024 * 1024 || !SourceKeyGenerator.sha256(verifiedArchive).equals(context.rawSha256())
                || !Set.of(2000, 2023).contains(year)) throw new IOException("ML_PILOT_ARCHIVE_PIN_REQUIRED");
        var parser = new JmaFixedWidthParser();
        SilverObservation centre = null;
        try (var reader = reader(verifiedArchive, context.memberName())) {
            String line; int physical = 0;
            while ((line = reader.readLine()) != null) {
                if (++physical > MAX_PHYSICAL_LINES) throw new IOException("ML_PILOT_SCAN_LIMIT");
                var parsed = parser.parseVerifiedRecord(line.getBytes(StandardCharsets.ISO_8859_1), physical, context);
                if (!parsed.observations().isEmpty() && mainshock(parsed.observations().get(0), year)) {
                    centre = parsed.observations().get(0); break; // physical order is the declared selection rule
                }
            }
        }
        if (centre == null) throw new IOException("ML_PILOT_MAINSHOCK_NOT_FOUND");
        Instant start = centre.eventTimeUtc().minus(Duration.ofDays(7)), end = centre.eventTimeUtc().plus(Duration.ofDays(7));
        var selected = new ArrayList<SilverObservation>(); var rejects = new ArrayList<SilverRejectRecord>();
        int physical = 0;
        try (var reader = reader(verifiedArchive, context.memberName())) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (++physical > MAX_PHYSICAL_LINES) throw new IOException("ML_PILOT_SCAN_LIMIT");
                byte[] record = line.getBytes(StandardCharsets.ISO_8859_1);
                Instant time = JmaFixedWidthParser.originTimeOfVerifiedRecord(record);
                if (time != null && (time.isBefore(start) || !time.isBefore(end))) continue;
                var parsed = parser.parseVerifiedRecord(record, physical, context);
                if (!parsed.observations().isEmpty()) {
                    var observation = parsed.observations().get(0);
                    if (!observation.eventTimeUtc().isBefore(start) && observation.eventTimeUtc().isBefore(end)) selected.add(observation);
                }
                // Invalid dates cannot be assigned to a temporal slice: retain parse rejects separately.
                rejects.addAll(parsed.rejects());
                if (selected.size() + rejects.size() > MAX_SELECTED) throw new IOException("ML_PILOT_SELECTED_LIMIT_EXCEEDED");
            }
        }
        if (selected.size() < 50) throw new IOException("ML_PILOT_SAMPLE_INSUFFICIENT");
        return new Sample(centre, List.copyOf(selected), List.copyOf(rejects), physical, start, end);
    }
    private static BufferedReader reader(byte[] archive, String member) throws IOException {
        var zip = new ZipInputStream(new ByteArrayInputStream(archive));
        var entry = zip.getNextEntry();
        if (entry == null || !member.equals(entry.getName())) { zip.close(); throw new IOException("ML_PILOT_MEMBER_CONFLICT"); }
        return new BufferedReader(new InputStreamReader(zip, StandardCharsets.ISO_8859_1));
    }
}
