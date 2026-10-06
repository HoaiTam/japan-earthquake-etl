package ie212.earthquake.spark.jma;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Reads the versioned CSV inventory without scraping the JMA HTML index. */
public final class JmaArchiveInventory {
    private static final List<String> REQUIRED_COLUMNS = List.of(
            "inventory_version", "year", "segment", "native_start_jst", "native_end_jst",
            "archive_name", "member_name", "source_url", "observed_release_hint",
            "observed_last_modified_utc", "observed_content_length_bytes", "media_type",
            "record_format", "catalog_era");

    private JmaArchiveInventory() {
    }

    public static List<JmaArchiveEntry> read(Path csv) throws IOException {
        Objects.requireNonNull(csv, "csv");
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("JMA inventory is empty: " + csv);
        }
        List<String> headers = parseLine(lines.get(0));
        if (!headers.equals(REQUIRED_COLUMNS)) {
            throw new IOException("JMA inventory header does not match the pinned contract");
        }
        List<JmaArchiveEntry> entries = new ArrayList<>();
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1);
            if (line.isBlank()) {
                continue;
            }
            if (lineNumber == 1) {
                continue;
            }
            List<String> values = parseLine(line);
            if (values.size() != headers.size()) {
                throw new IOException("JMA inventory row " + lineNumber + " has " + values.size()
                        + " columns, expected " + headers.size());
            }
            Map<String, String> row = headers.stream().collect(Collectors.toMap(
                    header -> header,
                    header -> values.get(headers.indexOf(header)),
                    (left, right) -> right));
            entries.add(new JmaArchiveEntry(
                    row.get("inventory_version"),
                    Integer.parseInt(row.get("year")),
                    row.get("segment"),
                    row.get("native_start_jst"),
                    row.get("native_end_jst"),
                    row.get("archive_name"),
                    row.get("member_name"),
                    URI.create(row.get("source_url")),
                    nullable(row.get("observed_release_hint")),
                    nullable(row.get("observed_last_modified_utc")),
                    parseLong(row.get("observed_content_length_bytes")),
                    row.get("media_type"),
                    row.get("record_format"),
                    row.get("catalog_era")));
        }
        return List.copyOf(entries);
    }

    private static Long parseLong(String value) throws IOException {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IOException("invalid content length in JMA inventory: " + value, exception);
        }
    }

    private static String nullable(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /** Small CSV parser supporting the quoted values allowed by the inventory contract. */
    static List<String> parseLine(String line) throws IOException {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);
            if (character == '"') {
                if (quoted && index + 1 < line.length() && line.charAt(index + 1) == '"') {
                    current.append('"');
                    index++;
                } else {
                    quoted = !quoted;
                }
            } else if (character == ',' && !quoted) {
                values.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }
        if (quoted) {
            throw new IOException("unterminated quoted CSV field");
        }
        values.add(current.toString().trim());
        return values;
    }
}
