package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Implementation of the Compression interface using the LZ4 compression algorithm.
 * <p>
 * Note: This implementation is relatively slow compared to other compression algorithms.
 * For example, compressing 100,000 elements (NoksDB) takes approximately 19,490 milliseconds,
 * and 10,000 elements (NoksDB) takes approximately 1,835 milliseconds.
 * <p>
 * Please do not use this without having the LZ4 and Apache Compress library loaded.
 */
public class LZ4Compression implements Compression {

    /**
     * Returns a new instance of the LZ4Compression class.
     *
     * @return the shared instance of LZ4Compression
     */
    public static LZ4Compression instance() {
        return new LZ4Compression();
    }

    @Override
    public byte[] compress(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            FramedLZ4CompressorOutputStream lzOut = new FramedLZ4CompressorOutputStream(bos);
            lzOut.write(data);
            lzOut.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error compressing data", e);
        }
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            FramedLZ4CompressorInputStream lzIn = new FramedLZ4CompressorInputStream(bis);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final byte[] buffer = new byte[length];
            int n;
            while (-1 != (n = lzIn.read(buffer))) {
                bos.write(buffer, 0, n);
            }
            lzIn.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error decompressing data", e);
        }
    }

    @Override
    public boolean writeLength() {
        return true;
    }
}