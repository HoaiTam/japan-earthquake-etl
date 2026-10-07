package ie212.earthquake.spark.jma;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Checks a complete ZIP and counts 96-byte records without decoding or extracting it. */
public final class JmaArchiveValidator {
    public static final long DEFAULT_MAX_ARCHIVE_BYTES = 128L * 1024 * 1024;
    public static final long DEFAULT_MAX_MEMBER_BYTES = 512L * 1024 * 1024;
    private final long maxArchiveBytes;
    private final long maxMemberBytes;

    public JmaArchiveValidator() {
        this(DEFAULT_MAX_ARCHIVE_BYTES, DEFAULT_MAX_MEMBER_BYTES);
    }

    public JmaArchiveValidator(long maxArchiveBytes, long maxMemberBytes) {
        if (maxArchiveBytes < 1 || maxMemberBytes < 1) {
            throw new IllegalArgumentException("ZIP limits must be positive");
        }
        this.maxArchiveBytes = maxArchiveBytes;
        this.maxMemberBytes = maxMemberBytes;
    }

    public JmaArchiveValidationResult validate(byte[] archive, String expectedMember) throws IOException {
        Objects.requireNonNull(archive, "archive");
        if (!safeMember(expectedMember)) {
            throw new IllegalArgumentException("expectedMember must be one safe file name");
        }
        if (archive.length > maxArchiveBytes) {
            return JmaArchiveValidationResult.rejected("ARCHIVE_SIZE_LIMIT");
        }
        // ZipFile requires the central directory, unlike ZipInputStream which may accept a truncated ZIP.
        Path temporary = Files.createTempFile("jma-03-validate-", ".zip");
        try {
            Files.write(temporary, archive);
            try (ZipFile zip = new ZipFile(temporary.toFile())) {
                if (zip.size() != 1) {
                    return JmaArchiveValidationResult.rejected("UNEXPECTED_MEMBER_COUNT");
                }
                ZipEntry member = zip.entries().nextElement();
                if (member.isDirectory() || !safeMember(member.getName())
                        || !expectedMember.equals(member.getName())) {
                    return JmaArchiveValidationResult.rejected("UNEXPECTED_MEMBER_NAME");
                }
                if (member.getSize() < 0 || member.getSize() > maxMemberBytes) {
                    return JmaArchiveValidationResult.rejected("MEMBER_SIZE_LIMIT");
                }
                CRC32 crc = new CRC32();
                long size = 0;
                long records = 0;
                int width = 0;
                boolean carriageReturn = false;
                boolean badRecord = false;
                byte[] buffer = new byte[8192];
                try (InputStream stream = zip.getInputStream(member)) {
                    int length;
                    while ((length = stream.read(buffer)) != -1) {
                        size += length;
                        if (size > maxMemberBytes) {
                            return JmaArchiveValidationResult.rejected("MEMBER_SIZE_LIMIT");
                        }
                        crc.update(buffer, 0, length);
                        for (int index = 0; index < length; index++) {
                            int value = buffer[index] & 0xff;
                            if (value == '\n') {
                                if (width != 96) {
                                    badRecord = true;
                                }
                                records++;
                                width = 0;
                                carriageReturn = false;
                            } else if (value == '\r') {
                                if (carriageReturn || width != 96) {
                                    badRecord = true;
                                }
                                carriageReturn = true;
                            } else {
                                if (carriageReturn) {
                                    badRecord = true;
                                }
                                // Saturate the counter; malformed long rows cannot overflow it.
                                width = Math.min(97, width + 1);
                            }
                        }
                    }
                }
                if (size != member.getSize() || crc.getValue() != member.getCrc()) {
                    return JmaArchiveValidationResult.rejected("ZIP_CRC_OR_SIZE_MISMATCH");
                }
                if (carriageReturn || (width != 0 && width != 96)) {
                    badRecord = true;
                }
                if (width > 0 && !carriageReturn) {
                    records++;
                }
                if (badRecord) {
                    return JmaArchiveValidationResult.rejected("INVALID_RECORD_LENGTH");
                }
                return new JmaArchiveValidationResult(true, records, null);
            } catch (IOException exception) {
                return JmaArchiveValidationResult.rejected("INVALID_ZIP");
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static boolean safeMember(String name) {
        return name != null && name.matches("[A-Za-z0-9][A-Za-z0-9._~-]*") && !name.contains("..");
    }
}
