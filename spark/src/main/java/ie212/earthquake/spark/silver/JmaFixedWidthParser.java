package ie212.earthquake.spark.silver;

import ie212.earthquake.spark.jma.JmaArchiveValidationResult;
import ie212.earthquake.spark.jma.JmaArchiveValidator;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipFile;

/** Byte-position JMA parser producing the existing Spark/Parquet Silver 1.0 models. */
public final class JmaFixedWidthParser {
    public static final String PARSER_NAME = "jma-hypocenter-fixed-width";
    public static final String PARSER_VERSION = "slv-03-v1";
    public static final String SOURCE_SYSTEM = "JMA_BULLETIN";
    private static final ZoneId JST = ZoneId.of("Asia/Tokyo");
    private static final LocalDateTime UNIFIED_START = LocalDateTime.of(1997, 10, 1, 0, 0);
    // CON-01 technical envelope; independent of the USGS parser implementation.
    private static final double ROI_MIN_LAT = 20.0;
    private static final double ROI_MAX_LAT = 50.0;
    private static final double ROI_MIN_LON = 120.0;
    private static final double ROI_MAX_LON = 155.0;
    private final long maxArchiveBytes;
    private final long maxMemberBytes;

    public JmaFixedWidthParser() {
        this(JmaArchiveValidator.DEFAULT_MAX_ARCHIVE_BYTES, JmaArchiveValidator.DEFAULT_MAX_MEMBER_BYTES);
    }

    public JmaFixedWidthParser(long maxArchiveBytes, long maxMemberBytes) {
        if (maxArchiveBytes < 1 || maxMemberBytes < 1) {
            throw new IllegalArgumentException("ZIP/member limits must be positive");
        }
        this.maxArchiveBytes = maxArchiveBytes;
        this.maxMemberBytes = maxMemberBytes;
    }

    public JmaParseResult parse(ResolvedBronzeInput input, String memberName,
            String sourceUrl, Instant processedAtUtc) throws IOException {
        return parse(input, JmaParseContext.of(input, memberName, sourceUrl, processedAtUtc));
    }

    /** Reverify the exact resolved ZIP; changed staging bytes never produce observations. */
    public JmaParseResult parse(ResolvedBronzeInput input, JmaParseContext context) throws IOException {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(context, "context");
        if (!SOURCE_SYSTEM.equals(input.sourceSystem())
                || !context.bronzeManifestId().equals(input.manifestId())
                || !context.catalogRelease().equals(input.catalogRelease())
                || !context.ingestRunId().equals(input.runId())
                || !context.rawObjectUri().equals(input.rawObjectUri())
                || !context.rawSha256().equals(input.sha256())) {
            throw new IllegalArgumentException("parse context must match the exact resolved JMA input");
        }
        if (Files.size(input.stagedObject()) != input.contentLengthBytes()) {
            throw new IOException("CONTRACT_MISMATCH: staged archive length changed");
        }
        return parseArchive(input.stagedObject(), context);
    }

    public JmaParseResult parseArchive(Path archivePath, JmaParseContext context) throws IOException {
        if (Files.size(archivePath) > maxArchiveBytes) {
            throw new IOException("ARCHIVE_SIZE_LIMIT");
        }
        return parseArchive(Files.readAllBytes(archivePath), context);
    }

    public JmaParseResult parseArchive(byte[] archive, JmaParseContext context) throws IOException {
        Objects.requireNonNull(archive, "archive");
        Objects.requireNonNull(context, "context");
        if (archive.length > maxArchiveBytes) {
            throw new IOException("ARCHIVE_SIZE_LIMIT");
        }
        if (!context.rawSha256().equals(SourceKeyGenerator.sha256(archive))) {
            throw new IOException("CHECKSUM_MISMATCH: archive does not match Bronze lineage");
        }
        JmaArchiveValidationResult validation = new JmaArchiveValidator(maxArchiveBytes, maxMemberBytes)
                .validate(archive, context.memberName());
        if (!validation.valid()) {
            throw new IOException("JMA archive rejected: " + validation.reason());
        }
        Path temporary = Files.createTempFile("slv-03-parse-", ".zip");
        try {
            Files.write(temporary, archive);
            try (ZipFile zip = new ZipFile(temporary.toFile());
                    InputStream stream = zip.getInputStream(zip.getEntry(context.memberName()))) {
                JmaParseResult result = parseRecords(stream, context);
                if (result.parsedCount() != validation.recordCount()) {
                    throw new IOException("CONTRACT_MISMATCH: archive count changed");
                }
                return result;
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** Direct fixed-width API for fixtures or an already verified member, not arbitrary Bronze ZIPs. */
    public JmaParseResult parse(Path memberPath, JmaParseContext context) throws IOException {
        try (InputStream stream = Files.newInputStream(memberPath)) {
            return parseRecords(stream, context);
        }
    }

    public JmaParseResult parse(byte[] member, JmaParseContext context) throws IOException {
        Objects.requireNonNull(member, "member");
        return parseRecords(new ByteArrayInputStream(member), context);
    }

    private JmaParseResult parseRecords(InputStream input, JmaParseContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        List<SilverObservation> observations = new ArrayList<>();
        List<SilverRejectRecord> rejects = new ArrayList<>();
        List<JmaNativeFields> nativeFields = new ArrayList<>();
        try (InputStream stream = new BufferedInputStream(input)) {
            byte[] row = new byte[96];
            int width = 0;
            int lineNumber = 1;
            long bytesRead = 0;
            int value;
            while ((value = stream.read()) != -1) {
                if (++bytesRead > maxMemberBytes) {
                    throw new IOException("MEMBER_SIZE_LIMIT");
                }
                if (value == '\r') {
                    if (stream.read() != '\n') {
                        throw new IOException("INVALID_RECORD_LENGTH: bare CR at line " + lineNumber);
                    }
                    if (++bytesRead > maxMemberBytes) {
                        throw new IOException("MEMBER_SIZE_LIMIT");
                    }
                    value = '\n';
                }
                if (value == '\n') {
                    requireWidth(width, lineNumber);
                    parseRecord(row, lineNumber++, context, observations, rejects, nativeFields);
                    width = 0;
                } else {
                    if (width == 96) {
                        throw new IOException("INVALID_RECORD_LENGTH: oversized line " + lineNumber);
                    }
                    row[width++] = (byte) value;
                }
            }
            if (width > 0) {
                requireWidth(width, lineNumber);
                parseRecord(row, lineNumber, context, observations, rejects, nativeFields);
            }
        }
        return new JmaParseResult(observations, rejects, nativeFields);
    }

    private static void requireWidth(int width, int lineNumber) throws IOException {
        if (width != 96) {
            throw new IOException("INVALID_RECORD_LENGTH: line " + lineNumber + " has " + width + " bytes");
        }
    }

    private static void parseRecord(byte[] bytes, int lineNumber, JmaParseContext context,
            List<SilverObservation> observations, List<SilverRejectRecord> rejects,
            List<JmaNativeFields> nativeFields) {
        // ISO-8859-1 is only a lossless byte-to-position bridge, NOT a source charset declaration.
        String line = new String(bytes, StandardCharsets.ISO_8859_1);
        String locator = "member=" + context.memberName() + ";line=" + lineNumber;
        String hash = SourceKeyGenerator.sha256(bytes);
        Set<String> reasons = new LinkedHashSet<>();
        Set<String> warnings = new LinkedHashSet<>();
        for (byte value : bytes) {
            if ((value & 0xff) < 32 || (value & 0xff) > 126) {
                reasons.add("CONTRACT_MISMATCH"); // Don't silently guess a charset or replace invalid bytes.
            }
        }
        String agency = code(field(line, 1, 1));
        if (agency == null || !JmaCodeMapping.AGENCIES.contains(agency)) {
            reasons.add("UNSUPPORTED_RECORD_TYPE");
        }
        LocalDateTime jst = originTime(line, reasons);
        Double latitude = coordinate(field(line, 22, 24), field(line, 25, 28), 90, "INVALID_LATITUDE", reasons);
        Double longitude = coordinate(field(line, 33, 36), field(line, 37, 40), 180, "INVALID_LONGITUDE", reasons);
        Double depth = optionalNumber(field(line, 45, 49), 2, reasons);
        Double magnitude1 = magnitude(field(line, 53, 54), reasons);
        Double magnitude2 = magnitude(field(line, 56, 57), reasons);
        Double magnitude = magnitude1 != null ? magnitude1 : magnitude2;
        String magnitudeType = code(magnitude1 != null ? field(line, 55, 55)
                : magnitude2 != null ? field(line, 58, 58) : " ");
        // Validate optional numeric fields too; blanks stay missing, not zero.
        optionalNumber(field(line, 18, 21), 2, reasons);
        optionalNumber(field(line, 29, 32), 2, reasons);
        optionalNumber(field(line, 41, 44), 2, reasons);
        optionalNumber(field(line, 50, 52), 2, reasons);
        optionalInteger(field(line, 65, 65), reasons);
        optionalInteger(field(line, 66, 68), reasons);
        optionalInteger(field(line, 93, 95), reasons);

        String subsidiary = code(field(line, 61, 61));
        String intensity = code(field(line, 62, 62));
        String tsunami = code(field(line, 64, 64));
        String determination = code(field(line, 96, 96));
        if (depth != null && depth < 0) warnings.add("NEGATIVE_DEPTH");
        if (magnitude != null && magnitudeType == null) warnings.add("MISSING_MAGNITUDE_TYPE");
        unknown(magnitudeType, JmaCodeMapping.MAGNITUDE_TYPES.keySet(), "UNKNOWN_MAGNITUDE_TYPE", warnings);
        unknown(subsidiary, JmaCodeMapping.EVENT_TYPES.keySet(), "UNKNOWN_EVENT_CATEGORY", warnings);
        unknown(intensity, JmaCodeMapping.INTENSITY_CODES, "UNKNOWN_INTENSITY_CODE", warnings);
        unknown(tsunami, JmaCodeMapping.TSUNAMI_CODES, "UNKNOWN_TSUNAMI_CODE", warnings);
        unknown(determination, JmaCodeMapping.DETERMINATION_FLAGS.keySet(), "UNKNOWN_DETERMINATION_FLAG", warnings);

        String key = agency != null && jst != null && latitude != null && longitude != null
                ? SourceKeyGenerator.jmaRecordKeyFromLine(line) : null;
        if (!reasons.isEmpty()) {
            rejects.add(new SilverRejectRecord("1.0", SOURCE_SYSTEM, key,
                    context.bronzeManifestId(), context.rawObjectUri(), context.rawSha256(), locator, hash,
                    "PARSE", List.copyOf(reasons), context.ingestRunId(), PARSER_VERSION, context.processedAtUtc()));
            return;
        }
        Instant utc = jst.atZone(JST).toInstant();
        LocalDate utcDate = utc.atZone(ZoneOffset.UTC).toLocalDate();
        LocalDateTime derivedJst = LocalDateTime.ofInstant(utc, JST);
        String revision = SourceKeyGenerator.jmaRevisionKey(context.catalogRelease(), hash);
        boolean inStudyArea = latitude >= ROI_MIN_LAT && latitude <= ROI_MAX_LAT
                && longitude >= ROI_MIN_LON && longitude <= ROI_MAX_LON;
        observations.add(new SilverObservation("1.0", SourceKeyGenerator.observationId(SOURCE_SYSTEM, key, revision),
                SOURCE_SYSTEM, key, revision, null, context.catalogRelease(), context.catalogReleaseAtUtc(), false,
                utc, derivedJst, utcDate, derivedJst.toLocalDate(), utcDate.getYear(), utcDate.getMonthValue(),
                latitude, longitude, depth, magnitude, magnitudeType, JmaCodeMapping.eventType(subsidiary),
                code(field(line, 69, 92)), JmaCodeMapping.tsunamiFlag(tsunami), null, null, intensity, agency,
                jst.isBefore(UNIFIED_START) ? "LEGACY" : "UNIFIED", determination, context.sourceUrl(),
                inStudyArea, warnings.isEmpty() ? "VALID" : "WARNING", List.copyOf(warnings),
                context.bronzeManifestId(), context.rawObjectUri(), context.rawSha256(), locator, hash,
                context.ingestRunId(), PARSER_NAME, PARSER_VERSION, context.processedAtUtc()));
        nativeFields.add(new JmaNativeFields(locator, field(line, 1, 1), field(line, 53, 54), field(line, 55, 55),
                field(line, 56, 57), field(line, 58, 58), field(line, 59, 59), field(line, 60, 60),
                field(line, 61, 61), field(line, 62, 62), field(line, 63, 63), field(line, 64, 64),
                field(line, 65, 65), field(line, 66, 68), field(line, 69, 92), field(line, 93, 95), field(line, 96, 96)));
    }

    private static LocalDateTime originTime(String line, Set<String> reasons) {
        try {
            int year = integer(field(line, 2, 5));
            Double seconds = decimal(field(line, 14, 17), 2);
            if (year < 1 || seconds == null || seconds < 0 || seconds >= 60) {
                throw new IllegalArgumentException("invalid year/second");
            }
            int hundredths = (int) Math.round(seconds * 100);
            return LocalDateTime.of(year, integer(field(line, 6, 7)),
                    integer(field(line, 8, 9)), integer(field(line, 10, 11)), integer(field(line, 12, 13)),
                    hundredths / 100, (hundredths % 100) * 10_000_000);
        } catch (RuntimeException exception) {
            reasons.add("INVALID_EVENT_TIME");
            return null;
        }
    }

    private static Double coordinate(String degreesRaw, String minutesRaw, int maximum,
            String reason, Set<String> reasons) {
        try {
            int degrees = integer(degreesRaw);
            Double minutes = decimal(minutesRaw, 2);
            if (minutes == null || minutes < 0 || minutes >= 60 || Math.abs(degrees) > maximum
                    || (Math.abs(degrees) == maximum && minutes != 0)) {
                throw new IllegalArgumentException("invalid degrees/minutes");
            }
            double value = Math.abs(degrees) + minutes / 60;
            return degreesRaw.stripLeading().startsWith("-") ? -value : value;
        } catch (RuntimeException exception) {
            reasons.add(reason);
            return null;
        }
    }

    private static Double optionalNumber(String raw, int scale, Set<String> reasons) {
        try {
            return decimal(raw, scale);
        } catch (RuntimeException exception) {
            reasons.add("INVALID_NUMBER");
            return null;
        }
    }

    private static Double magnitude(String raw, Set<String> reasons) {
        if (raw.matches("[A-C][0-9]")) {
            return -(raw.charAt(0) - 'A' + 1) - (raw.charAt(1) - '0') / 10.0;
        }
        return optionalNumber(raw, 1, reasons);
    }

    private static void optionalInteger(String raw, Set<String> reasons) {
        if (!raw.isBlank()) {
            try {
                if (integer(raw) < 0) throw new IllegalArgumentException("negative code/count");
            } catch (RuntimeException exception) {
                reasons.add("INVALID_NUMBER");
            }
        }
    }

    private static int integer(String raw) {
        if (!raw.trim().matches("[+-]?[0-9]+")) throw new IllegalArgumentException("invalid integer");
        return Integer.parseInt(raw.trim());
    }

    /** Fortran Fw.d: explicit decimal wins; trailing blank fractional places are zero positions. */
    private static Double decimal(String raw, int scale) {
        if (raw.isBlank()) return null;
        String text = raw.stripLeading();
        if (text.contains(".")) {
            text = text.trim();
            if (!text.matches("[+-]?(?:[0-9]+(?:\\.[0-9]{0,2})?|\\.[0-9]{1,2})")) {
                throw new IllegalArgumentException("invalid explicit decimal");
            }
            return Double.parseDouble(text);
        }
        if (!text.matches("[+-]?[0-9]+ *")) throw new IllegalArgumentException("invalid implied decimal");
        return Double.parseDouble(text.replace(' ', '0')) / Math.pow(10, scale);
    }

    private static String field(String line, int start, int end) { return line.substring(start - 1, end); }
    private static String code(String raw) { return raw.isBlank() ? null : raw.trim(); }
    private static void unknown(String value, Set<String> known, String warning, Set<String> warnings) {
        if (value != null && !known.contains(value)) warnings.add(warning);
    }
}
