package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;

/**
 * An implementation of the Compression interface that does not compress data.
 */
public class NoCompression implements Compression {

    /**
     * Creates a new instance of this class.
     *
     * @return a NoCompression instance
     */
    public static Compression instance() {
        return new NoCompression();
    }

    @Override
    public byte[] compress(byte[] data) {
        return data;
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        return data;
    }

    @Override
    public boolean writeLength() {
        return false;
    }
}
