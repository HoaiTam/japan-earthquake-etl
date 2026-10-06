package ie212.earthquake.spark.jma;

import java.net.URI;

public record JmaHttpMetadata(
        int statusCode,
        String contentType,
        Long contentLengthBytes,
        String etag,
        String lastModified,
        URI finalUri) {
}
