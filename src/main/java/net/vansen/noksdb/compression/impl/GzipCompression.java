package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Implementation of the Compression interface using the GZIP compression algorithm.
 */
public class GzipCompression implements Compression {

    /**
     * Returns a new instance of the GzipCompression class.
     *
     * @return a new instance of GzipCompression
     */
    public static GzipCompression instance() {
        return new GzipCompression();
    }

    @Override
    public byte[] compress(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzOut = new GZIPOutputStream(bos);
            gzOut.write(data);
            gzOut.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error compressing data", e);
        }
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream gzIn = new GZIPInputStream(bis);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final byte[] buffer = new byte[length];
            int n;
            while (-1 != (n = gzIn.read(buffer))) {
                bos.write(buffer, 0, n);
            }
            gzIn.close();
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