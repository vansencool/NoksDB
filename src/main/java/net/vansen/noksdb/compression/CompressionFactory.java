package net.vansen.noksdb.compression;

import net.vansen.noksdb.compression.impl.DeflateCompression;
import net.vansen.noksdb.compression.impl.GzipCompression;
import net.vansen.noksdb.compression.impl.LZMACompression;
import net.vansen.noksdb.compression.impl.SnappyCompression;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Deflater;

/**
 * A factory class for creating and retrieving Compression instances.
 * <p>
 * Please do not use this without having the libraries for the compression algorithms loaded.
 *
 * @see CompressionBased
 */
@SuppressWarnings("unused")
public class CompressionFactory {

    /**
     * A map of registered Compression instances, keyed by name.
     */
    private static final Map<String, Compression> compressions = new ConcurrentHashMap<>();

    /**
     * Registers a Compression instance with the given name.
     *
     * @param name        the name of the Compression instance
     * @param compression the Compression instance to register
     */
    public static void registerCompression(String name, Compression compression) {
        compressions.put(name, compression);
    }

    /**
     * Retrieves a Compression instance by name.
     *
     * @param name the name of the Compression instance to retrieve
     * @return the Compression instance with the given name, or null if not found
     */
    public static Compression getCompression(String name) {
        return compressions.get(name);
    }

    /**
     * Returns an instance of the Snappy compression algorithm.
     *
     * @return a Snappy Compression instance
     */
    public static Compression getSnappyCompression() {
        return SnappyCompression.instance();
    }

    /**
     * Returns an instance of the LZMA compression algorithm.
     *
     * @return an LZMA Compression instance
     */
    public static Compression getLZMACompression() {
        return LZMACompression.instance();
    }

    /**
     * Returns an instance of the Gzip compression algorithm.
     *
     * @return a Gzip Compression instance
     */
    public static Compression getGzipCompression() {
        return GzipCompression.instance();
    }

    /**
     * Returns an instance of the Deflate compression algorithm.
     *
     * @return a Deflate Compression instance
     */
    public static Compression getDeflateCompression() {
        return DeflateCompression.instance();
    }

    /**
     * Returns an instance of the Deflate compression algorithm with the specified compression level.
     *
     * @param level the compression level to use (see {@link Deflater})
     * @return a Deflate Compression instance
     */
    public static Compression getDeflateCompression(int level) {
        return DeflateCompression.instance(level);
    }
}