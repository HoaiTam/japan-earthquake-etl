package ie212.earthquake.spark.jma;

import java.net.URI;
import java.util.Arrays;

public record JmaHttpPayload(
        int statusCode,
        String contentType,
        Long contentLengthBytes,
        String etag,
        String lastModified,
        URI finalUri,
        byte[] body,
        String contentRange) {

    public JmaHttpPayload(int statusCode, String contentType, Long contentLengthBytes,
            String etag, String lastModified, URI finalUri, byte[] body) {
        this(statusCode, contentType, contentLengthBytes, etag, lastModified, finalUri, body, null);
    }

    public JmaHttpPayload {
        body = body == null ? new byte[0] : Arrays.copyOf(body, body.length);
    }

    @Override
    public byte[] body() {
        return Arrays.copyOf(body, body.length);
    }
}
