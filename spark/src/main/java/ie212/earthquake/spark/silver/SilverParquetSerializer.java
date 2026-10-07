package ie212.earthquake.spark.silver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Serializes Silver observations and reject records into standard Apache Parquet format (CON-03 1.0).
 * Runs purely in JVM without requiring native Hadoop binaries or winutils.
 */
public final class SilverParquetSerializer {

    private static final ZoneId JST_ZONE = ZoneId.of("Asia/Tokyo");

    public static final MessageType OBSERVATION_PARQUET_SCHEMA = MessageTypeParser.parseMessageType(
            "message source_observation {\n" +
            "  required binary schema_version (UTF8);\n" +
            "  required binary source_observation_id (UTF8);\n" +
            "  required binary source_system (UTF8);\n" +
            "  required binary source_record_key (UTF8);\n" +
            "  required binary source_revision_key (UTF8);\n" +
            "  optional int64 source_updated_at_utc (TIMESTAMP_MILLIS);\n" +
            "  optional binary catalog_release (UTF8);\n" +
            "  optional int64 catalog_release_at_utc (TIMESTAMP_MILLIS);\n" +
            "  required boolean is_current_source_revision;\n" +
            "  required int64 event_time_utc (TIMESTAMP_MILLIS);\n" +
            "  required int64 event_time_jst (TIMESTAMP_MILLIS);\n" +
            "  required int32 event_date_utc (DATE);\n" +
            "  required int32 event_date_jst (DATE);\n" +
            "  required int32 event_year_utc;\n" +
            "  required int32 event_month_utc;\n" +
            "  required double latitude;\n" +
            "  required double longitude;\n" +
            "  optional double depth_km;\n" +
            "  optional double magnitude;\n" +
            "  optional binary magnitude_type (UTF8);\n" +
            "  required binary event_type_code (UTF8);\n" +
            "  optional binary place_name (UTF8);\n" +
            "  optional boolean tsunami_flag;\n" +
            "  optional binary alert_level (UTF8);\n" +
            "  optional int32 significance;\n" +
            "  optional binary max_intensity_code (UTF8);\n" +
            "  optional binary determining_agency_code (UTF8);\n" +
            "  optional binary catalog_era (UTF8);\n" +
            "  optional binary source_status (UTF8);\n" +
            "  optional binary source_url (UTF8);\n" +
            "  required boolean is_in_study_area;\n" +
            "  required binary quality_status (UTF8);\n" +
            "  required group quality_flags (LIST) {\n" +
            "    repeated group list {\n" +
            "      required binary element (UTF8);\n" +
            "    }\n" +
            "  }\n" +
            "  required binary bronze_manifest_id (UTF8);\n" +
            "  required binary raw_object_uri (UTF8);\n" +
            "  required binary raw_sha256 (UTF8);\n" +
            "  required binary raw_record_locator (UTF8);\n" +
            "  required binary raw_record_hash (UTF8);\n" +
            "  required binary ingest_run_id (UTF8);\n" +
            "  required binary parser_name (UTF8);\n" +
            "  required binary parser_version (UTF8);\n" +
            "  required int64 processed_at_utc (TIMESTAMP_MILLIS);\n" +
            "}");

    public static final MessageType REJECT_PARQUET_SCHEMA = MessageTypeParser.parseMessageType(
            "message reject_record {\n" +
            "  required binary schema_version (UTF8);\n" +
            "  required binary source_system (UTF8);\n" +
            "  optional binary source_record_key_candidate (UTF8);\n" +
            "  required binary bronze_manifest_id (UTF8);\n" +
            "  required binary raw_object_uri (UTF8);\n" +
            "  required binary raw_sha256 (UTF8);\n" +
            "  required binary raw_record_locator (UTF8);\n" +
            "  required binary raw_record_hash (UTF8);\n" +
            "  required binary reject_stage (UTF8);\n" +
            "  required group reject_reason_codes (LIST) {\n" +
            "    repeated group list {\n" +
            "      required binary element (UTF8);\n" +
            "    }\n" +
            "  }\n" +
            "  required binary ingest_run_id (UTF8);\n" +
            "  required binary parser_version (UTF8);\n" +
            "  required int64 rejected_at_utc (TIMESTAMP_MILLIS);\n" +
            "}");

    /**
     * Serializes a list of SilverObservation records to uncompressed Parquet byte array.
     */
    public byte[] serializeObservations(List<SilverObservation> observations) throws IOException {
        Objects.requireNonNull(observations, "observations");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        InMemoryOutputFile outputFile = new InMemoryOutputFile(buffer);
        Configuration conf = new Configuration();

        try (ParquetWriter<Group> writer = new SimpleGroupParquetWriterBuilder(outputFile, OBSERVATION_PARQUET_SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(OBSERVATION_PARQUET_SCHEMA);
            for (SilverObservation obs : observations) {
                Group group = groupFactory.newGroup();

                group.append("schema_version", obs.schemaVersion());
                group.append("source_observation_id", obs.sourceObservationId());
                group.append("source_system", obs.sourceSystem());
                group.append("source_record_key", obs.sourceRecordKey());
                group.append("source_revision_key", obs.sourceRevisionKey());

                if (obs.sourceUpdatedAtUtc() != null) {
                    group.append("source_updated_at_utc", obs.sourceUpdatedAtUtc().toEpochMilli());
                }
                if (obs.catalogRelease() != null) {
                    group.append("catalog_release", obs.catalogRelease());
                }
                if (obs.catalogReleaseAtUtc() != null) {
                    group.append("catalog_release_at_utc", obs.catalogReleaseAtUtc().toEpochMilli());
                }

                group.append("is_current_source_revision", obs.isCurrentSourceRevision());
                group.append("event_time_utc", obs.eventTimeUtc().toEpochMilli());

                long eventTimeJstMillis = obs.eventTimeJst().atZone(JST_ZONE).toInstant().toEpochMilli();
                group.append("event_time_jst", eventTimeJstMillis);

                group.append("event_date_utc", (int) obs.eventDateUtc().toEpochDay());
                group.append("event_date_jst", (int) obs.eventDateJst().toEpochDay());
                group.append("event_year_utc", obs.eventYearUtc());
                group.append("event_month_utc", obs.eventMonthUtc());

                group.append("latitude", obs.latitude());
                group.append("longitude", obs.longitude());

                if (obs.depthKm() != null) {
                    group.append("depth_km", obs.depthKm());
                }
                if (obs.magnitude() != null) {
                    group.append("magnitude", obs.magnitude());
                }
                if (obs.magnitudeType() != null) {
                    group.append("magnitude_type", obs.magnitudeType());
                }

                group.append("event_type_code", obs.eventTypeCode());

                if (obs.placeName() != null) {
                    group.append("place_name", obs.placeName());
                }
                if (obs.tsunamiFlag() != null) {
                    group.append("tsunami_flag", obs.tsunamiFlag());
                }
                if (obs.alertLevel() != null) {
                    group.append("alert_level", obs.alertLevel());
                }
                if (obs.significance() != null) {
                    group.append("significance", obs.significance());
                }
                if (obs.maxIntensityCode() != null) {
                    group.append("max_intensity_code", obs.maxIntensityCode());
                }
                if (obs.determiningAgencyCode() != null) {
                    group.append("determining_agency_code", obs.determiningAgencyCode());
                }
                if (obs.catalogEra() != null) {
                    group.append("catalog_era", obs.catalogEra());
                }
                if (obs.sourceStatus() != null) {
                    group.append("source_status", obs.sourceStatus());
                }
                if (obs.sourceUrl() != null) {
                    group.append("source_url", obs.sourceUrl());
                }

                group.append("is_in_study_area", obs.isInStudyArea());
                group.append("quality_status", obs.qualityStatus());

                Group qualityFlagsGroup = group.addGroup("quality_flags");
                for (String flag : obs.qualityFlags()) {
                    qualityFlagsGroup.addGroup("list").append("element", flag);
                }

                group.append("bronze_manifest_id", obs.bronzeManifestId());
                group.append("raw_object_uri", obs.rawObjectUri());
                group.append("raw_sha256", obs.rawSha256());
                group.append("raw_record_locator", obs.rawRecordLocator());
                group.append("raw_record_hash", obs.rawRecordHash());
                group.append("ingest_run_id", obs.ingestRunId());
                group.append("parser_name", obs.parserName());
                group.append("parser_version", obs.parserVersion());
                group.append("processed_at_utc", obs.processedAtUtc().toEpochMilli());

                writer.write(group);
            }
        }

        return buffer.toByteArray();
    }

    /**
     * Serializes a list of SilverRejectRecord records to uncompressed Parquet byte array.
     */
    public byte[] serializeRejects(List<SilverRejectRecord> rejects) throws IOException {
        Objects.requireNonNull(rejects, "rejects");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        InMemoryOutputFile outputFile = new InMemoryOutputFile(buffer);
        Configuration conf = new Configuration();

        try (ParquetWriter<Group> writer = new SimpleGroupParquetWriterBuilder(outputFile, REJECT_PARQUET_SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(REJECT_PARQUET_SCHEMA);
            for (SilverRejectRecord reject : rejects) {
                Group group = groupFactory.newGroup();

                group.append("schema_version", reject.schemaVersion());
                group.append("source_system", reject.sourceSystem());

                if (reject.sourceRecordKeyCandidate() != null) {
                    group.append("source_record_key_candidate", reject.sourceRecordKeyCandidate());
                }

                group.append("bronze_manifest_id", reject.bronzeManifestId());
                group.append("raw_object_uri", reject.rawObjectUri());
                group.append("raw_sha256", reject.rawSha256());
                group.append("raw_record_locator", reject.rawRecordLocator());
                group.append("raw_record_hash", reject.rawRecordHash());
                group.append("reject_stage", reject.rejectStage());

                Group reasonsGroup = group.addGroup("reject_reason_codes");
                for (String code : reject.rejectReasonCodes()) {
                    reasonsGroup.addGroup("list").append("element", code);
                }

                group.append("ingest_run_id", reject.ingestRunId());
                group.append("parser_version", reject.parserVersion());
                group.append("rejected_at_utc", reject.rejectedAtUtc().toEpochMilli());

                writer.write(group);
            }
        }

        return buffer.toByteArray();
    }

    /**
     * Verifies that the given Parquet bytes contain the expected row count and schema.
     */
    public static void verifyParquet(byte[] parquetBytes, int expectedRecordCount, MessageType expectedSchema)
            throws IOException {
        Objects.requireNonNull(parquetBytes, "parquetBytes");
        InMemoryInputFile inputFile = new InMemoryInputFile(parquetBytes);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            long actualCount = reader.getRecordCount();
            if (actualCount != expectedRecordCount) {
                throw new IOException(String.format(
                        "Parquet record count mismatch: expected %d, got %d", expectedRecordCount, actualCount));
            }
            if (expectedSchema != null) {
                MessageType actualSchema = reader.getFileMetaData().getSchema();
                if (!actualSchema.containsAllFields(expectedSchema.asGroupType())) {
                    throw new IOException("Parquet schema does not contain all expected fields: " + actualSchema);
                }
            }
        }
    }

    // --- In-memory Parquet I/O Support ---

    static final class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream outputStream;

        InMemoryOutputFile(ByteArrayOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new InMemoryPositionOutputStream(outputStream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 128 * 1024 * 1024;
        }

        @Override
        public String getPath() {
            return "in-memory.parquet";
        }
    }

    static final class InMemoryPositionOutputStream extends PositionOutputStream {
        private final OutputStream out;
        private long position = 0;

        InMemoryPositionOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    static final class InMemoryInputFile implements InputFile {
        private final byte[] bytes;

        InMemoryInputFile(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getLength() {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new ByteArrayInputStream(bytes)) {
                private long position = 0;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void seek(long newPos) {
                    this.position = newPos;
                    ((ByteArrayInputStream) getStream()).reset();
                    long skipped = ((ByteArrayInputStream) getStream()).skip(newPos);
                }

                @Override
                public int read() {
                    int val = super.read();
                    if (val != -1) position++;
                    return val;
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    int bytesRead = super.read(b, off, len);
                    if (bytesRead > 0) position += bytesRead;
                    return bytesRead;
                }
            };
        }
    }

    static final class SimpleGroupParquetWriterBuilder
            extends ParquetWriter.Builder<Group, SimpleGroupParquetWriterBuilder> {
        private final MessageType schema;

        SimpleGroupParquetWriterBuilder(OutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected SimpleGroupParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
            GroupWriteSupport.setSchema(schema, conf);
            return new GroupWriteSupport();
        }
    }
}
