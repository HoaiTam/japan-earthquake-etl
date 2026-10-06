package ie212.earthquake.spark.jma;

import java.io.IOException;
import java.net.URI;

/** HTTP boundary used by the downloader so tests never need the live JMA service. */
public interface JmaArchiveTransport {
    JmaHttpMetadata head(URI uri) throws IOException, InterruptedException;

    JmaHttpPayload get(URI uri, long rangeStart) throws IOException, InterruptedException;
}
