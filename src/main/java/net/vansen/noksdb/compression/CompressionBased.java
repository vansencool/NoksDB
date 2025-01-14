package net.vansen.noksdb.compression;

import net.vansen.noksdb.compression.impl.LZMACompression;
import net.vansen.noksdb.compression.impl.NoCompression;
import net.vansen.noksdb.compression.impl.SnappyCompression;

/**
 * Utility class for creating compression instances based on the availability of the libraries.
 */
@SuppressWarnings("unused")
public class CompressionBased {

    /**
     * Creates a new instance of the SnappyCompression class if the Snappy library is available, or a NoCompression instance otherwise.
     *
     * @return a Compression instance, either SnappyCompression or NoCompression
     */
    public static Compression snappyOrNone() {
        try {
            Class.forName("org.xerial.snappy.Snappy");
            return SnappyCompression.instance();
        } catch (Exception e) {
            return new NoCompression();
        }
    }

    /**
     * Creates a new instance of the LZMACompression class if the LZMA library is available, or a NoCompression instance otherwise.
     *
     * @return a Compression instance, either LZMACompression or NoCompression
     */
    public static Compression lzmaOrNone() {
        try {
            Class.forName("org.tukaani.xz.LZMAOutputStream");
            return LZMACompression.instance();
        } catch (Exception e) {
            return new NoCompression();
        }
    }
}
