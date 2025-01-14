package net.vansen.noksdb.compression.impl;

import net.vansen.noksdb.compression.Compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Implementation of the Compression interface using the Deflater and Inflater classes.
 */
public class DeflateCompression implements Compression {

    /**
     * The default compression level used by this class.
     */
    private static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;

    /**
     * The compression level used by this instance.
     */
    private final int level;

    /**
     * Creates a new instance of this class with the specified compression level.
     *
     * @param level the compression level to use (see {@link Deflater})
     */
    public DeflateCompression(int level) {
        this.level = level;
    }

    /**
     * Creates a new instance of this class with the default compression level.
     */
    public DeflateCompression() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance of this class with the default compression level.
     *
     * @return a Deflate Compression instance
     */
    public static Compression instance() {
        return new DeflateCompression();
    }

    /**
     * Creates a new instance of this class with the specified compression level.
     *
     * @param level the compression level to use (see {@link Deflater})
     * @return a Deflate Compression instance
     */
    public static Compression instance(int level) {
        return new DeflateCompression(level);
    }

    @Override
    public byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        Deflater compressor = new Deflater();
        compressor.setLevel(level);
        compressor.setInput(data);
        compressor.finish();
        byte[] buf = new byte[data.length];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
        compressor.end();
        byte[] compressed = bos.toByteArray();
        bos.close();
        return compressed;
    }

    @Override
    public byte[] decompress(byte[] compressed, int length) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
            Inflater decompressor = new Inflater();
            decompressor.setInput(compressed);
            byte[] buf = new byte[length];
            ByteArrayOutputStream bos = new ByteArrayOutputStream(compressed.length);
            while (!decompressor.finished()) {
                int count = decompressor.inflate(buf);
                bos.write(buf, 0, count);
            }
            decompressor.end();
            byte[] decompressed = bos.toByteArray();
            bos.close();
            bis.close();
            return decompressed;
        } catch (Exception e) {
            throw new RuntimeException("Error decompressing data", e);
        }
    }

    @Override
    public boolean writeLength() {
        return true;
    }
}
