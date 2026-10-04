package ie212.earthquake.spark.usgs;

import java.io.IOException;

/** Minimal immutable object-store boundary used by the Bronze writer. */
public interface BronzeObjectStore {
    /** Writes a new object and refuses an existing key. */
    void putIfAbsent(String key, byte[] payload, String contentType) throws IOException;

    /** Reads the complete object for checksum/readback verification. */
    byte[] read(String key) throws IOException;

    /** Checks whether an object key already exists. */
    boolean exists(String key) throws IOException;

    /** Resolves a key to the URI written into a manifest. */
    String uriForKey(String key);
}
