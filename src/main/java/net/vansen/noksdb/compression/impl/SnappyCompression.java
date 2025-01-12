package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;
import org.xerial.snappy.Snappy;

/**
 * Implementation of the Compression interface using the Snappy compression algorithm.
 * <p>
 * Please do not use this without having the Snappy library loaded.
 */
public class SnappyCompression implements Compression {

    /**
     * Returns a new instance of the SnappyCompression class.
     *
     * @return a new instance of SnappyCompression
     */
    public static SnappyCompression instance() {
        return new SnappyCompression();
    }

    @Override
    public byte[] compress(byte[] data) {
        try {
            return Snappy.compress(data);
        } catch (Exception e) {
            throw new RuntimeException("Error compressing data", e);
        }
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        try {
            return Snappy.uncompress(data);
        } catch (Exception e) {
            throw new RuntimeException("Error decompressing data", e);
        }
    }

    @Override
    public boolean writeLength() {
        return false;
    }
}