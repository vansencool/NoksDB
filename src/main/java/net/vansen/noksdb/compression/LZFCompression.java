package net.vansen.noksdb.compression;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import org.h2.compress.LZFInputStream;
import org.h2.compress.LZFOutputStream;
import org.jetbrains.annotations.ApiStatus;

/**
 * A compression algorithm that uses the LZF (Lempel-Ziv-Factor) algorithm to compress and decompress data.
 * <p>
 * Note: This implementation has not been thoroughly tested and may not work as expected.
 * <p>
 * In particular, the following compression ratios have been observed:
 * <ul>
 *     <li>1 KB for 1, 10, 100, 1000, 2000, 3000, 4000 elements</li>
 *     <li>39 KB for 5000, 6000, 7000, 8000, and 9000 elements</li>
 *     <li>86 KB for 10000 elements</li>
 * </ul>
 * <p>
 * These results are not consistent and may indicate a problem with the implementation.
 * <p>
 * However, as-checked from testing, it appears to work well for elements above 10000, anything below 10000 is causing NullPointerException(s).
 * <p>
 * At the end, please use with caution.
 */
@ApiStatus.Experimental
public class LZFCompression implements Compression {

    /**
     * Compresses the given data using the LZF algorithm.
     */
    @Override
    public byte[] compress(byte[] data) {
        try (FastByteArrayOutputStream output = new FastByteArrayOutputStream();
             LZFOutputStream lzf = new LZFOutputStream(output)) {
            lzf.write(data);
            return output.array;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decompresses the given data using the LZF algorithm.
     */
    @Override
    public byte[] decompress(byte[] bytes, int length) {
        try (FastByteArrayInputStream input = new FastByteArrayInputStream(bytes);
             LZFInputStream lzf = new LZFInputStream(input);
             FastByteArrayOutputStream output = new FastByteArrayOutputStream()) {
            lzf.transferTo(output);
            return output.array;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean writeLength() {
        return false;
    }
}