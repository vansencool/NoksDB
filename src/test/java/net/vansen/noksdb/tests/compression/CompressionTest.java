package net.vansen.noksdb.tests.compression;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.compression.ZstdCompression;

import java.io.File;

public class CompressionTest {
    private static Compression compresser;

    public static void main(String[] args) {
        compresser = new ZstdCompression();
        compressBenchmarks();
        ensureWorks(100);
    }

    public static void compressBenchmarks() {
        int[] numIterations = {1, 100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 100000, 1000000};

        System.out.println("Put performance tests:");
        for (int numIter : numIterations) {
            // NoksDB
            NoksDB db = NoksDB.builder()
                    .storageFile(new File("tests\\noksdb compressed " + numIter + ".dat"))
                    .autoSave(false)
                    .compression(compresser)
                    .build();

            long startTimeNs = System.nanoTime();
            long startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                db.rowOf("user" + i)
                        .value("balance", i)
                        .insert();
            }
            long endTimeNs = System.nanoTime();
            long endTimeMs = System.currentTimeMillis();
            long durationNs = endTimeNs - startTimeNs;
            long durationMs = endTimeMs - startTimeMs;
            System.out.println("NoksDB:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            db.save();
            db.close();


            /*
            // SQLite
            Connection conn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite " + numIter + ".db");
            PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();
            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            startTimeNs = System.nanoTime();
            startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.executeUpdate();
            }
            endTimeNs = System.nanoTime();
            endTimeMs = System.currentTimeMillis();
            durationNs = endTimeNs - startTimeNs;
            durationMs = endTimeMs - startTimeMs;
            System.out.println("SQLite:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            conn.close();

            // H2
            conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDB\\tests\\h2 " + numIter + ".db");
            stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();
            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            startTimeNs = System.nanoTime();
            startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.executeUpdate();
            }
            endTimeNs = System.nanoTime();
            endTimeMs = System.currentTimeMillis();
            durationNs = endTimeNs - startTimeNs;
            durationMs = endTimeMs - startTimeMs;
            System.out.println("H2:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            conn.close();
            System.out.println("-".repeat(50));
            System.out.println();

             */

        }
    }

    public static void ensureWorks(int num) {
        NoksDB db = NoksDB.builder()
                .storageFile(new File("tests\\noksdb compressed " + num + ".dat"))
                .autoSave(false)
                .compression(compresser)
                .build();

        System.out.println(db.fetch("user50").field("balance").get());
    }
}
