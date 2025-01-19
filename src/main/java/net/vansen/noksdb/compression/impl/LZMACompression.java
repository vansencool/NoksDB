package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;

import java.io.*;

/**
 * Implementation of the Compression interface using the LZMA compression algorithm.
 * <p>
 * Please do not use this without having the XZ library loaded.
 */
public class LZMACompression implements Compression {

    /**
     * Returns a new instance of the LZMACompression class.
     *
     * @return a new instance of LZMACompression
     */
    public static LZMACompression instance() {
        return new LZMACompression();
    }

    @Override
    public byte[] compress(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            BufferedOutputStream out = new BufferedOutputStream(bos);
            LZMACompressorOutputStream lzOut = new LZMACompressorOutputStream(out);
            lzOut.write(data);
            lzOut.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        try {
            LZMACompressorInputStream lzIn = new LZMACompressorInputStream(new BufferedInputStream(new ByteArrayInputStream(data)));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final byte[] buffer = new byte[length];
            int n;
            while (-1 != (n = lzIn.read(buffer))) {
                bos.write(buffer, 0, n);
            }
            lzIn.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean writeLength() {
        return true;
    }
}