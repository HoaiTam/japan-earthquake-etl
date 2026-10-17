package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Reads a result bundle from an exact, bounded directory before it reaches a writer. */
public final class MlResultBundleReader {
    public static final long MAX_FILE_BYTES = 128L * 1024 * 1024;
    public static final int MAX_FILES = 1024;
    private static final ObjectMapper JSON = new ObjectMapper();

    private MlResultBundleReader() { }

    public static ResultBundleValidator.DecodedBundle read(Path source) throws Exception {
        Path root = exactDirectory(source);
        Map<String, byte[]> artifacts = new TreeMap<>();
        for (JsonNode item : ResultBundleContract.contract().path("required_artifacts")) {
            String name = item.asText();
            artifacts.put(name, readSafe(root, name));
        }
        try (var paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                String name = root.relativize(path).toString().replace('\\', '/');
                if (name.equals("_SUCCESS.json") || artifacts.containsKey(name)) return;
                if (!name.startsWith("optional-model-artifacts/") || !name.matches("optional-model-artifacts/[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)?"))
                    throw new IllegalArgumentException("IMP_UNSAFE_ARTIFACT_METADATA");
                try { artifacts.put(name, readSafe(root, name)); }
                catch (IOException error) { throw new IllegalArgumentException("IMP_ARTIFACT_READ_FAILED", error); }
            });
        }
        if (artifacts.size() > MAX_FILES) throw new IllegalArgumentException("IMP_ARTIFACT_LIMIT");
        JsonNode success = readJson(root, "_SUCCESS.json");
        List<JsonNode> memberships = readParquet(root, "memberships.parquet");
        List<JsonNode> summaries = readParquet(root, "sequence_summary.parquet");
        return new ResultBundleValidator.DecodedBundle(success, artifacts, memberships, summaries, true);
    }

    private static Path exactDirectory(Path source) throws IOException {
        if (source == null || Files.isSymbolicLink(source) || !Files.isDirectory(source))
            throw new IllegalArgumentException("IMP_ARTIFACT_ROOT_INVALID");
        return source.toRealPath(LinkOption.NOFOLLOW_LINKS);
    }

    private static byte[] readSafe(Path root, String relative) throws IOException {
        Path path = root.resolve(relative).normalize();
        if (!path.startsWith(root) || Files.isSymbolicLink(path) || !Files.isRegularFile(path))
            throw new IllegalArgumentException("IMP_ARTIFACT_PATH_INVALID");
        long size = Files.size(path);
        if (size > MAX_FILE_BYTES) throw new IllegalArgumentException("IMP_ARTIFACT_LIMIT");
        return Files.readAllBytes(path);
    }

    private static JsonNode readJson(Path root, String name) throws IOException {
        return JSON.readTree(readSafe(root, name));
    }

    private static List<JsonNode> readParquet(Path root, String artifact) throws Exception {
        Path path = root.resolve(artifact);
        MessageType expected = schema(artifact);
        List<JsonNode> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(input(path))) {
            MessageType actual = reader.getFooter().getFileMetaData().getSchema();
            if (!actual.equals(expected) && !actual.getFields().equals(expected.getFields()))
                throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
            var pages = reader.readNextRowGroup();
            while (pages != null) {
                var records = new ColumnIOFactory().getColumnIO(actual).getRecordReader(pages, new GroupRecordConverter(actual));
                for (long index = 0; index < pages.getRowCount(); index++) rows.add(row(records.read(), artifact));
                pages = reader.readNextRowGroup();
            }
        }
        return List.copyOf(rows);
    }

    private static InputFile input(Path path) {
        return new InputFile() {
            public long getLength() throws IOException { return Files.size(path); }
            public SeekableInputStream newStream() throws IOException {
                var file = new java.io.RandomAccessFile(path.toFile(), "r");
                return new DelegatingSeekableInputStream(new java.io.InputStream() {
                    public int read() throws IOException { return file.read(); }
                    public int read(byte[] bytes, int offset, int length) throws IOException { return file.read(bytes, offset, length); }
                    public void close() throws IOException { file.close(); }
                }) {
                    public long getPos() throws IOException { return file.getFilePointer(); }
                    public void seek(long position) throws IOException { file.seek(position); }
                };
            }
        };
    }

    private static MessageType schema(String artifact) {
        StringBuilder text = new StringBuilder("message result { ");
        for (JsonNode field : ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
            String repetition = field.path("nullable").asBoolean() ? "optional" : "required";
            String type = field.path("type").asText(), key = field.path("name").asText();
            if (type.equals("array<string>")) {
                text.append(repetition).append(" group ").append(key).append(" (LIST) { repeated group list { required binary element (UTF8); } } ");
                continue;
            }
            String physical = switch (type) {
                case "string", "json_string" -> "binary";
                case "boolean" -> "boolean";
                case "integer" -> "int32";
                case "long", "timestamp_utc" -> "int64";
                case "double" -> "double";
                default -> throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
            };
            text.append(repetition).append(' ').append(physical).append(' ').append(key);
            if (type.equals("string") || type.equals("json_string")) text.append(" (UTF8)");
            if (type.equals("timestamp_utc")) text.append(" (TIMESTAMP(MILLIS,true))");
            text.append("; ");
        }
        return MessageTypeParser.parseMessageType(text.append("}").toString());
    }

    private static ObjectNode row(Group group, String artifact) {
        ObjectNode row = JSON.createObjectNode();
        for (JsonNode field : ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
            String key = field.path("name").asText(), type = field.path("type").asText();
            if (group.getFieldRepetitionCount(key) == 0) { row.putNull(key); continue; }
            switch (type) {
                case "string", "json_string" -> row.put(key, group.getBinary(key, 0).toStringUsingUTF8());
                case "timestamp_utc" -> row.put(key, Instant.ofEpochMilli(group.getLong(key, 0)).toString());
                case "boolean" -> row.put(key, group.getBoolean(key, 0));
                case "integer" -> row.put(key, group.getInteger(key, 0));
                case "long" -> row.put(key, group.getLong(key, 0));
                case "double" -> row.put(key, group.getDouble(key, 0));
                case "array<string>" -> {
                    ArrayNode array = row.putArray(key); Group list = group.getGroup(key, 0);
                    for (int i = 0; i < list.getFieldRepetitionCount("list"); i++) array.add(list.getGroup("list", i).getBinary("element", 0).toStringUsingUTF8());
                }
                default -> throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
            }
        }
        return row;
    }
}
