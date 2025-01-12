package net.vansen.noksdb.tests.compression;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.compression.impl.*;
import oshi.SystemInfo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
public class CompressionTest {
    public static void main(String[] args) throws IOException {
        List<Compression> compressors = List.of(
                new NoCompression(), // No compression, bare test
                new LZ4Compression(), // Worst performance, not recommended at all
                new LZMACompression(), // Second-best compression, little bad performance
                new SnappyCompression(), // Fastest, half size as none,but basically same speed as none (1 ms difference in 100k iterations)
                new GzipCompression(), // Slower than Snappy about 4x, very good compression
                new DeflateCompression() // Depends on compression level
        );

        int[] numIterations = {1, 10, 100, 1000, 10000, 100000};
        byte[] testData = ("testing " + "x".repeat(10000) + "COMPRESS_BENCH").getBytes();

        SystemInfo si = new SystemInfo();

        if (!new File("compression").exists()) new File("compression").mkdir();
        try (PrintWriter writer = new PrintWriter("compression/results.log")) {
            writer.println("Information:");
            writer.println("  Physical Cores: " + si.getHardware().getProcessor().getPhysicalProcessorCount());
            writer.println("  Logical Cores: " + si.getHardware().getProcessor().getLogicalProcessorCount());
            writer.println("  Allocated Memory: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");
            writer.println("  Max Memory: " + (si.getHardware().getMemory().getTotal() / (1024 * 1024)) + " MB");
            writer.println("  CPU Model: " + si.getHardware().getProcessor().getProcessorIdentifier().getName());
            writer.println("  Memory type: " + si.getHardware().getMemory().getPhysicalMemory().getFirst().getMemoryType());
            writer.println("  Operating System: " + si.getOperatingSystem().getFamily());
            writer.println();

            for (Compression compressor : compressors) {
                System.out.println("Testing Compressor: " + compressor.getClass().getSimpleName());
                benchmarkCompressor(compressor, numIterations, testData, writer);
                System.out.println("=".repeat(80));
            }
        }
    }

    private static void benchmarkCompressor(Compression compressor, int[] numIterations, byte[] testData, PrintWriter writer) throws IOException {
        String compressorName = compressor.getClass().getSimpleName().replace("Compression", "");

        long compressionStart = System.nanoTime();
        byte[] compressedData = compressor.compress(testData);
        long compressionEnd = System.nanoTime();

        long decompressionStart = System.nanoTime();
        byte[] decompressedData = compressor.decompress(compressedData, testData.length);
        long decompressionEnd = System.nanoTime();

        boolean dataMatches = new String(testData).equals(new String(decompressedData));
        if (!new File("compression").exists()) new File("compression").mkdir();
        writer.println("Compression Algorithm: " + compressor.getClass().getSimpleName());
        writer.println();
        writer.printf("Original Size: %d bytes -> Compressed Size: %d bytes\n", testData.length, compressedData.length);
        writer.printf("Compression Time: %.3f ms\n", (compressionEnd - compressionStart) / 1e6);
        writer.printf("Decompression Time: %.3f ms\n", (decompressionEnd - decompressionStart) / 1e6);
        writer.printf("Compression Ratio: %.3f\n", (double) compressedData.length / testData.length);
        writer.printf("Decompressed length (bytes): %d\n", decompressedData.length);
        writer.println("Data Integrity: " + (dataMatches ? "PASS" : "FAIL"));
        writer.println();

        writer.println("NoksDB Compression Benchmarks:");
        for (int numIter : numIterations) {
            File dbFile = new File("tests/compressed/noksdb_compressed_" + compressorName + "_" + numIter + ".dat");
            if (dbFile.exists()) dbFile.delete();

            NoksDB db = NoksDB.builder()
                    .storageFile(dbFile)
                    .autoSave(false)
                    .compression(compressor)
                    .build();

            for (int i = 0; i < numIter; i++) {
                db.rowOf("user" + i).value("balance", i).insert();
            }

            long startTimeNs = System.nanoTime();
            db.save();
            long endTimeNs = System.nanoTime();
            double saveTimeMs = (endTimeNs - startTimeNs) / 1e6;

            startTimeNs = System.nanoTime();
            NoksDB loadedDb = NoksDB.builder()
                    .storageFile(dbFile)
                    .autoSave(false)
                    .compression(compressor)
                    .build();
            endTimeNs = System.nanoTime();
            double loadTimeMs = (endTimeNs - startTimeNs) / 1e6;

            writer.printf("Iterations: %d | Save Time: %.3f ms | Load Time: %.3f ms\n",
                    numIter, saveTimeMs, loadTimeMs);

            System.out.printf("Iterations: %d | Save Time: %.3f ms | Load Time: %.3f ms\n",
                    numIter, saveTimeMs, loadTimeMs);
        }
        writer.println();
    }
}