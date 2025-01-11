package net.vansen.noksdb.compression;

/**
 * Interface for compression algorithms used in NoksDB.
 * <p>
 * Implementations of this interface provide methods for compressing and decompressing byte arrays.
 * <p>
 * The compressed data will be stored in a file, reducing the overall size of the file, but increasing the time it takes to read and write data (depending on the compression algorithm).
 * <p>
 * It is very recommended to make the compression algorithm thread-safe, as it may be used asynchronously.
 */
public interface Compression {

    /**
     * Compresses the given byte array.
     *
     * @param data the byte array to compress
     * @return the compressed byte array
     */
    byte[] compress(byte[] data);

    /**
     * Decompresses the given byte array.
     * <p>
     * The length parameter is required for compression algorithms that need to know the original size of the data, such as ZSTD and LZ4, so it's here for convenience.
     *
     * @param data   the compressed byte array
     * @param length the original size of the byte array before compression, MAY be 0 if writeLength() returns false
     * @return the decompressed byte array
     */
    byte[] decompress(byte[] data, int length);

    /**
     * Returns true if the compression algorithm needs to know the original size of the data, such as ZSTD and LZ4.
     *
     * @return true if the compression algorithm needs to know the original size of the data, false otherwise
     */
    boolean writeLength();
}