package ie212.earthquake.spark.silver;

import java.io.IOException;

/** Reads exactly one raw Bronze object selected by a verified manifest. */
@FunctionalInterface
public interface BronzeInputReader {
    byte[] read(String rawObjectKey, String rawObjectUri) throws IOException;
}
