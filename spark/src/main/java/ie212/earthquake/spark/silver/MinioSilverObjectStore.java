package ie212.earthquake.spark.silver;

import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * MinIO/S3-backed implementation of SilverObjectStore using pipeline credentials.
 */
public final class MinioSilverObjectStore implements SilverObjectStore, AutoCloseable {

    private static final Pattern BUCKET_NAME = Pattern.compile(
            "(?=.{3,63}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?");
    private static final Pattern PREFIX = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._/-]*");

    interface Operations {
        void stat(String bucket, String key) throws Exception;
        void put(String bucket, String key, byte[] payload, String contentType) throws Exception;
        byte[] read(String bucket, String key) throws Exception;
        List<String> list(String bucket, String prefix) throws Exception;
        void delete(String bucket, String key) throws Exception;
    }

    private final Operations operations;
    private final String bucket;
    private final String silverPrefix;

    private MinioSilverObjectStore(Operations operations, String bucket, String silverPrefix) {
        this.operations = Objects.requireNonNull(operations, "operations");
        this.bucket = validateBucket(bucket);
        this.silverPrefix = validatePrefix(silverPrefix);
    }

    public static MinioSilverObjectStore fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    public static MinioSilverObjectStore fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String endpoint = required(environment, "MINIO_ENDPOINT");
        validateEndpoint(endpoint);
        String accessKey = required(environment, "MINIO_ACCESS_KEY");
        String secretKey = required(environment, "MINIO_SECRET_KEY");
        String bucket = required(environment, "DATA_BUCKET");
        String silverPrefix = environment.getOrDefault("SILVER_PREFIX", "silver").trim();
        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
        return new MinioSilverObjectStore(new SdkOperations(client), bucket, silverPrefix);
    }

    static MinioSilverObjectStore forTests(Operations operations, String bucket, String silverPrefix) {
        return new MinioSilverObjectStore(operations, bucket, silverPrefix);
    }

    @Override
    public void put(String key, byte[] content, String contentType) throws IOException {
        String fullKey = resolveKey(key);
        Objects.requireNonNull(content, "content");
        try {
            operations.put(bucket, fullKey, content, contentType != null ? contentType : "application/octet-stream");
        } catch (Exception ex) {
            throw new IOException("MinIO Silver put failed for key: " + fullKey, ex);
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        String fullKey = resolveKey(key);
        try {
            return operations.read(bucket, fullKey);
        } catch (Exception ex) {
            throw new IOException("MinIO Silver read failed for key: " + fullKey, ex);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        String fullKey = resolveKey(key);
        try {
            operations.stat(bucket, fullKey);
            return true;
        } catch (ErrorResponseException ex) {
            if (isMissing(ex)) {
                return false;
            }
            throw new IOException("MinIO Silver stat failed for key: " + fullKey, ex);
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public List<String> list(String prefix) throws IOException {
        String fullPrefix = prefix != null && !prefix.isBlank() ? resolveKey(prefix) : silverPrefix;
        try {
            List<String> rawKeys = operations.list(bucket, fullPrefix);
            List<String> relKeys = new ArrayList<>();
            String base = silverPrefix + "/";
            for (String k : rawKeys) {
                if (k.startsWith(base)) {
                    relKeys.add(k.substring(base.length()));
                } else {
                    relKeys.add(k);
                }
            }
            Collections.sort(relKeys);
            return relKeys;
        } catch (Exception ex) {
            throw new IOException("MinIO Silver list failed for prefix: " + fullPrefix, ex);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        String fullKey = resolveKey(key);
        try {
            operations.delete(bucket, fullKey);
        } catch (Exception ex) {
            throw new IOException("MinIO Silver delete failed for key: " + fullKey, ex);
        }
    }

    @Override
    public void deletePrefix(String prefix) throws IOException {
        String fullPrefix = resolveKey(prefix);
        try {
            List<String> keys = operations.list(bucket, fullPrefix);
            for (String key : keys) {
                operations.delete(bucket, key);
            }
        } catch (Exception ex) {
            throw new IOException("MinIO Silver deletePrefix failed for prefix: " + fullPrefix, ex);
        }
    }

    @Override
    public void move(String sourceKey, String targetKey) throws IOException {
        String fullSource = resolveKey(sourceKey);
        String fullTarget = resolveKey(targetKey);
        try {
            byte[] bytes = operations.read(bucket, fullSource);
            operations.put(bucket, fullTarget, bytes, "application/octet-stream");
            operations.delete(bucket, fullSource);
        } catch (Exception ex) {
            throw new IOException("MinIO Silver move failed from " + fullSource + " to " + fullTarget, ex);
        }
    }

    @Override
    public String uriForKey(String key) {
        return "s3://" + bucket + "/" + resolveKey(key);
    }

    @Override
    public void close() throws IOException {
        if (operations instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ex) {
                throw new IOException("MinIO Silver client close failed", ex);
            }
        }
    }

    private String resolveKey(String key) {
        if (key == null) {
            return silverPrefix;
        }
        String clean = key.trim().replace('\\', '/');
        while (clean.startsWith("/")) {
            clean = clean.substring(1);
        }
        if (clean.contains("..")) {
            throw new IllegalArgumentException("Silver key contains invalid '..' segment: " + key);
        }
        if (clean.startsWith(silverPrefix + "/")) {
            return clean;
        }
        return silverPrefix + "/" + clean;
    }

    private static String validateBucket(String value) {
        if (value == null || !BUCKET_NAME.matcher(value).matches()) {
            throw new IllegalArgumentException("DATA_BUCKET is not a valid S3 bucket name: " + value);
        }
        return value;
    }

    private static String validatePrefix(String value) {
        if (value == null
                || !PREFIX.matcher(value).matches()
                || value.startsWith("/")
                || value.endsWith("/")
                || value.contains("..")
                || value.contains("//")) {
            throw new IllegalArgumentException("SILVER_PREFIX must be a safe object prefix: " + value);
        }
        return value;
    }

    private static void validateEndpoint(String value) {
        try {
            URI endpoint = URI.create(value);
            if (!("http".equalsIgnoreCase(endpoint.getScheme()) || "https".equalsIgnoreCase(endpoint.getScheme()))
                    || endpoint.getHost() == null) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("MINIO_ENDPOINT must be a valid HTTP(S) URI: " + value);
        }
    }

    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required and must not be blank");
        }
        return value.trim();
    }

    private static boolean isMissing(ErrorResponseException exception) {
        String code = exception.errorResponse() == null ? null : exception.errorResponse().code();
        return "NoSuchKey".equals(code) || "NoSuchObject".equals(code);
    }

    private static final class SdkOperations implements Operations, AutoCloseable {
        private final MinioClient client;

        SdkOperations(MinioClient client) {
            this.client = Objects.requireNonNull(client, "client");
        }

        @Override
        public void stat(String bucket, String key) throws Exception {
            client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
        }

        @Override
        public void put(String bucket, String key, byte[] payload, String contentType) throws Exception {
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .data(payload, payload.length)
                    .contentType(contentType)
                    .build());
        }

        @Override
        public byte[] read(String bucket, String key) throws Exception {
            try (GetObjectResponse response = client.getObject(
                    GetObjectArgs.builder().bucket(bucket).object(key).build())) {
                return response.readAllBytes();
            }
        }

        @Override
        public List<String> list(String bucket, String prefix) throws Exception {
            List<String> keys = new ArrayList<>();
            Iterable<Result<Item>> results = client.listObjects(
                    ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build());
            for (Result<Item> item : results) {
                keys.add(item.get().objectName());
            }
            return keys;
        }

        @Override
        public void delete(String bucket, String key) throws Exception {
            client.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(key).build());
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }
}
