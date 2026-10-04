package ie212.earthquake.spark.usgs;

import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** MinIO-backed immutable Bronze object store using pipeline-scoped credentials. */
public final class MinioBronzeObjectStore implements BronzeObjectStore, AutoCloseable {
    private static final Pattern BUCKET_NAME = Pattern.compile(
            "(?=.{3,63}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?");
    private static final Pattern PREFIX = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._/-]*");

    interface Operations {
        void stat(String bucket, String key) throws Exception;

        void putIfAbsent(String bucket, String key, byte[] payload, String contentType)
                throws Exception;

        byte[] read(String bucket, String key) throws Exception;
    }

    private final Operations operations;
    private final String bucket;
    private final String bronzePrefix;

    private MinioBronzeObjectStore(
            Operations operations,
            String bucket,
            String bronzePrefix) {
        this.operations = Objects.requireNonNull(operations, "operations");
        this.bucket = validateBucket(bucket);
        this.bronzePrefix = validatePrefix(bronzePrefix);
    }

    /** Creates the adapter from the process environment without logging credentials. */
    public static MinioBronzeObjectStore fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /** Creates the adapter from an explicit environment for deterministic validation/tests. */
    public static MinioBronzeObjectStore fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String endpoint = required(environment, "MINIO_ENDPOINT");
        validateEndpoint(endpoint);
        String accessKey = required(environment, "MINIO_ACCESS_KEY");
        String secretKey = required(environment, "MINIO_SECRET_KEY");
        String bucket = required(environment, "DATA_BUCKET");
        String bronzePrefix = required(environment, "BRONZE_PREFIX");
        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
        return new MinioBronzeObjectStore(new SdkOperations(client), bucket, bronzePrefix);
    }

    static MinioBronzeObjectStore forTests(
            Operations operations,
            String bucket,
            String bronzePrefix) {
        return new MinioBronzeObjectStore(operations, bucket, bronzePrefix);
    }

    @Override
    public void putIfAbsent(String key, byte[] payload, String contentType) throws IOException {
        String safeKey = validateKey(key);
        Objects.requireNonNull(payload, "payload");
        if (contentType == null || contentType.isBlank()) {
            throw new IllegalArgumentException("contentType must not be blank");
        }
        try {
            operations.putIfAbsent(bucket, safeKey, payload, contentType);
        } catch (FileAlreadyExistsException exception) {
            throw exception;
        } catch (Exception exception) {
            throw storageFailure("write", safeKey, exception);
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        String safeKey = validateKey(key);
        try {
            return operations.read(bucket, safeKey);
        } catch (Exception exception) {
            throw storageFailure("read", safeKey, exception);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        String safeKey = validateKey(key);
        try {
            operations.stat(bucket, safeKey);
            return true;
        } catch (ErrorResponseException exception) {
            if (isMissing(exception)) {
                return false;
            }
            throw storageFailure("stat", safeKey, exception);
        } catch (ObjectMissingException exception) {
            return false;
        } catch (Exception exception) {
            throw storageFailure("stat", safeKey, exception);
        }
    }

    @Override
    public String uriForKey(String key) {
        return "s3://" + bucket + "/" + validateKey(key);
    }

    /** Releases the MinIO SDK HTTP resources so short-lived runner processes exit promptly. */
    @Override
    public void close() throws IOException {
        if (operations instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception exception) {
                throw new IOException("MinIO Bronze client close failed", exception);
            }
        }
    }

    private String validateKey(String key) {
        if (key == null
                || key.isBlank()
                || key.startsWith("/")
                || key.endsWith("/")
                || key.contains("..")
                || key.contains("\\")
                || !key.startsWith(bronzePrefix + "/")) {
            throw new IllegalArgumentException(
                    "Bronze object key must be a safe path under " + bronzePrefix + "/");
        }
        return key;
    }

    private static String validateBucket(String value) {
        if (value == null || !BUCKET_NAME.matcher(value).matches()) {
            throw new IllegalArgumentException("DATA_BUCKET is not a valid S3 bucket name");
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
            throw new IllegalArgumentException("BRONZE_PREFIX must be a safe object prefix");
        }
        return value;
    }

    private static void validateEndpoint(String value) {
        try {
            URI endpoint = URI.create(value);
            if (!("http".equalsIgnoreCase(endpoint.getScheme())
                    || "https".equalsIgnoreCase(endpoint.getScheme()))
                    || endpoint.getHost() == null
                    || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null
                    || endpoint.getFragment() != null) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "MINIO_ENDPOINT must be an HTTP(S) URI without query, fragment or credentials");
        }
    }

    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required and must not be blank");
        }
        return value.trim();
    }

    private static IOException storageFailure(String operation, String key, Exception cause) {
        return new IOException("MinIO Bronze " + operation + " failed for key " + key, cause);
    }

    private static boolean isMissing(ErrorResponseException exception) {
        String code = exception.errorResponse() == null
                ? null
                : exception.errorResponse().code();
        return "NoSuchKey".equals(code) || "NoSuchObject".equals(code);
    }

    static final class ObjectMissingException extends Exception {
        ObjectMissingException(String key) {
            super(key);
        }
    }

    private static final class SdkOperations implements Operations, AutoCloseable {
        private final MinioClient client;

        private SdkOperations(MinioClient client) {
            this.client = Objects.requireNonNull(client, "client");
        }

        @Override
        public void stat(String bucket, String key) throws Exception {
            client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
        }

        @Override
        public void putIfAbsent(
                String bucket,
                String key,
                byte[] payload,
                String contentType)
                throws Exception {
            try {
                client.putObject(PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(key)
                        .data(payload, payload.length)
                        .contentType(contentType)
                        .headers(Map.of("If-None-Match", "*"))
                        .build());
            } catch (ErrorResponseException exception) {
                String code = exception.errorResponse() == null
                        ? null
                        : exception.errorResponse().code();
                if ("PreconditionFailed".equals(code)) {
                    throw new FileAlreadyExistsException(key);
                }
                throw exception;
            }
        }

        @Override
        public byte[] read(String bucket, String key) throws Exception {
            try (GetObjectResponse response = client.getObject(
                    GetObjectArgs.builder().bucket(bucket).object(key).build())) {
                return response.readAllBytes();
            }
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }
}
