package net.vansen.noksdb.test;

import net.vansen.noksdb.NoksDBMap;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;

public class LocalSQLVsNoksDB {

    /*
    public static void main(String[] args) throws SQLException {
        int[] numIterations = {1, 100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 100000, 1000000};
        int warmup = 10;

        System.out.println("Put performance test:");
        for (int numIter : numIterations) {


            // NoksDBMap
            NoksDBMap db = NoksDBMap.builder()
                    .storageFile(new File("tests\\noksdb " + numIter + ".dat"))
                    .autoSave(false)
                    .build();

            for (int i = 0; i < warmup; i++) {
                db.createRow("user" + i)
                        .value("balance", i)
                        .insert();
                db.delete("user" + i);
            }

            long startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                db.createRow("user" + i)
                        .value("balance", i)
                        .insert();
            }
            db.save();
            long endTimeNs = System.nanoTime();
            long durationNs = endTimeNs - startTimeNs;
            long durationMs = (endTimeNs - startTimeNs) / 1_000_000;
            System.out.println("NoksDBMap:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            db.close();


            // SQLite
            Connection conn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite " + numIter + ".db");
            PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();
            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            conn.setAutoCommit(false);

            for (int i = 0; i < warmup; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.clearBatch();

            startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            endTimeNs = System.nanoTime();
            durationNs = endTimeNs - startTimeNs;
            durationMs = (endTimeNs - startTimeNs) / 1_000_000;
            System.out.println("SQLite:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            conn.close();


            // H2
            conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDBMap\\tests\\h2 " + numIter + ".db");
            stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();

            conn.setAutoCommit(false);

            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            for (int i = 0; i < warmup; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.clearBatch();

            startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            endTimeNs = System.nanoTime();
            durationNs = endTimeNs - startTimeNs;
            durationMs = (endTimeNs - startTimeNs) / 1_000_000;
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
        }
    }

    */

    /*
    public static void main(String[] args) throws SQLException, IOException {
        int[] numIterations = {1, 100, 1000, 2000, 3000, 4000, 5000};
        int warmup = 10;

        File outputFile = new File("benchmark_results.txt");
        FileWriter writer = new FileWriter(outputFile);
        writer.write("Benchmarking Results (NoksDBMap, SQLite, H2):\n\n");

        // Mermaid Diagram Setup
        StringBuilder mermaidBuilder = new StringBuilder();
        mermaidBuilder.append("```mermaid\n");
        mermaidBuilder.append("bar\n");
        mermaidBuilder.append("    title Database Benchmark Results\n");
        mermaidBuilder.append("    axisX Iterations\n");
        mermaidBuilder.append("    axisY Time (ms)\n");

        for (int numIter : numIterations) {
            writer.write("Iterations: " + numIter + "\n");
            writer.write(String.format("%-12s %-12s %-12s %-18s %-18s %-12s\n",
                    "Database", "Time (ms)", "Time (ns)", "Avg/Iter (ms)", "Avg/Iter (ns)", "Rank"));

            BenchmarkResult[] results = new BenchmarkResult[3];

            // NoksDBMap Benchmark
            NoksDBMap db = NoksDBMap.builder()
                    .storageFile(new File("tests\\noksdb_" + numIter + ".dat"))
                    .autoSave(false)
                    .build();
            warmupNoksDB(db, warmup);

            long startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                db.createRow("user" + i)
                        .value("balance", i)
                        .insert();
            }
            db.save();
            long durationNs = System.nanoTime() - startTimeNs;
            db.close();

            results[0] = new BenchmarkResult("NoksDBMap", durationNs, numIter);

            // SQLite Benchmark
            Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite_" + numIter + ".db");
            PreparedStatement sqliteStmt = sqliteConn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            sqliteStmt.execute();
            sqliteStmt = sqliteConn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
            warmupSQL(sqliteStmt, warmup);

            sqliteConn.setAutoCommit(false);
            startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                sqliteStmt.setInt(1, i);
                sqliteStmt.addBatch();
            }
            sqliteStmt.executeBatch();
            sqliteConn.commit();
            durationNs = System.nanoTime() - startTimeNs;
            sqliteConn.close();

            results[1] = new BenchmarkResult("SQLite", durationNs, numIter);

            // H2 Benchmark
            Connection h2Conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDBMap\\tests\\h2_" + numIter + ".db");
            PreparedStatement h2Stmt = h2Conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            h2Stmt.execute();
            h2Stmt = h2Conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
            warmupSQL(h2Stmt, warmup);

            h2Conn.setAutoCommit(false);
            startTimeNs = System.nanoTime();
            for (int i = 0; i < numIter; i++) {
                h2Stmt.setInt(1, i);
                h2Stmt.addBatch();
            }
            h2Stmt.executeBatch();
            h2Conn.commit();
            durationNs = System.nanoTime() - startTimeNs;
            h2Conn.close();

            results[2] = new BenchmarkResult("H2", durationNs, numIter);

            // Sort and Rank Results
            Arrays.sort(results, Comparator.comparingLong(BenchmarkResult::getTimeNs));
            for (int i = 0; i < results.length; i++) {
                results[i].setRank(i + 1);
            }

            // Write Results to File
            for (BenchmarkResult result : results) {
                writer.write(String.format("%-12s %-12.2f %-12d %-18.2f %-18.2f %-12d\n",
                        result.getDatabase(),
                        result.getTimeMs(),
                        result.getTimeNs(),
                        result.getTimePerIterationMs(),
                        result.getTimePerIterationNs(),
                        result.getRank()));

                // Append to Mermaid Diagram
                mermaidBuilder.append("    ").append(result.getDatabase()).append("_").append(numIter)
                        .append(": ").append(result.getTimeMs()).append("\n");
            }
            writer.write("\n");
            mermaidBuilder.append("\n");
        }

        writer.close();

        // Finalize Mermaid Diagram
        mermaidBuilder.append("```\n");

        // Write Mermaid Diagram to File
        File diagramFile = new File("benchmark_diagram.md");
        FileWriter diagramWriter = new FileWriter(diagramFile);
        diagramWriter.write(mermaidBuilder.toString());
        diagramWriter.close();

        System.out.println("Benchmark complete! Results written to 'benchmark_results.txt' and 'benchmark_diagram.md'.");
    }

    private static void warmupNoksDB(NoksDBMap db, int warmup) {
        for (int i = 0; i < warmup; i++) {
            db.createRow("user" + i)
                    .value("balance", i)
                    .insert();
            db.delete("user" + i);
        }
    }

    private static void warmupSQL(PreparedStatement stmt, int warmup) throws SQLException {
        for (int i = 0; i < warmup; i++) {
            stmt.setInt(1, i);
            stmt.addBatch();
        }
        stmt.clearBatch();
    }

    static class BenchmarkResult {
        private final String database;
        private final long timeNs;
        private final int numIterations;
        private int rank;

        public BenchmarkResult(String database, long timeNs, int numIterations) {
            this.database = database;
            this.timeNs = timeNs;
            this.numIterations = numIterations;
        }

        public String getDatabase() {
            return database;
        }

        public long getTimeNs() {
            return timeNs;
        }

        public double getTimeMs() {
            return timeNs / 1_000_000.0;
        }

        public double getTimePerIterationNs() {
            return timeNs / (double) numIterations;
        }

        public double getTimePerIterationMs() {
            return getTimePerIterationNs() / 1_000_000.0;
        }

        public int getRank() {
            return rank;
        }

        public void setRank(int rank) {
            this.rank = rank;
        }
    }
     */

    /*
    public static void main(String[] args) throws Exception {
        int[] numIterations = {
                1, 3, 7, 10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 350, 400, 450, 500,
                550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 2000, 3000, 4000, 5000,
                6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000,
                80000, 90000, 100000, 150000, 200000, 250000, 300000, 350000, 400000,
                450000, 500000, 550000, 600000, 650000, 700000, 750000, 800000, 850000,
                900000, 950000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000,
                7000000, 8000000, 9000000, 10000000, 20000000, 30000000, 40000000,
                50000000, 60000000, 70000000, 80000000, 90000000, 100000000
        };
        int warmup = 10;

        File resultsFile = new File("results\\benchmark_results.txt");
        if (resultsFile.exists()) resultsFile.delete();
        RocksDB.loadLibrary();
        new File("tests").mkdirs();
        new File("results").mkdirs();

        try (var writer = new FileWriter(resultsFile, true)) {
            writer.write("Benchmarking Results (NoksDBMap, SQLite, H2, RocksDB):\n\n");

            for (int numIter : numIterations) {
                writer.write("Iterations: " + numIter + "\n");
                writer.write(String.format("%-12s %-12s %-18s %-18s %-12s\n",
                        "Database", "Time (ms)", "Avg/Iter (ms)", "Avg/Iter (ns)", "Rank"));

                BenchmarkResult[] results = new BenchmarkResult[5];

                // NoksDBMap
                NoksDBMap db = NoksDBMap.builder()
                        .storageFile(new File("tests\\noksdb_" + numIter + ".dat"))
                        .autoSave(false)
                        .build();
                warmupNoksDB(db, warmup);

                long startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    db.createRow("user" + i)
                            .value("balance", i)
                            .insert();
                }
                long durationNs = System.nanoTime() - startTimeNs;
                db.close();
                results[0] = new BenchmarkResult("NoksDBMap", durationNs, numIter);

                // SQLite
                Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite_" + numIter + ".db");
                PreparedStatement sqliteStmt = sqliteConn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
                sqliteStmt.execute();
                sqliteStmt = sqliteConn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
                warmupSQL(sqliteStmt, warmup);

                sqliteConn.setAutoCommit(false);
                startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    sqliteStmt.setInt(1, i);
                    sqliteStmt.addBatch();
                }
                sqliteStmt.executeBatch();
                durationNs = System.nanoTime() - startTimeNs;
                sqliteConn.rollback();
                sqliteConn.close();
                results[1] = new BenchmarkResult("SQLite", durationNs, numIter);

                // H2
                Connection h2Conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDBMap\\tests\\h2_" + numIter);
                PreparedStatement h2Stmt = h2Conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
                h2Stmt.execute();
                h2Stmt = h2Conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
                warmupSQL(h2Stmt, warmup);

                h2Conn.setAutoCommit(false);
                startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    h2Stmt.setInt(1, i);
                    h2Stmt.addBatch();
                }
                h2Stmt.executeBatch();
                durationNs = System.nanoTime() - startTimeNs;
                h2Conn.rollback();
                h2Conn.close();
                results[2] = new BenchmarkResult("H2", durationNs, numIter);

                // RocksDB
                Options options = new Options()
                        .optimizeLevelStyleCompaction()
                        .setIncreaseParallelism(8)
                        .setCompressionType(CompressionType.NO_COMPRESSION)
                        .setCreateIfMissing(true);
                try (RocksDB rocksDB = RocksDB.open(options, "tests/rocksdb_" + numIter)) {
                    warmupRocksDB(rocksDB, warmup);

                    startTimeNs = System.nanoTime();
                    WriteBatch write = new WriteBatch();
                    for (int i = 0; i < numIter; i++) {
                        write.put(("user" + i).getBytes(), String.valueOf(i).getBytes());
                    }
                    WriteOptions writeOptions = new WriteOptions().setDisableWAL(true).setNoSlowdown(true);
                    rocksDB.write(writeOptions, write);
                    durationNs = System.nanoTime() - startTimeNs;
                    writeOptions.close();
                }
                results[3] = new BenchmarkResult("RocksDB", durationNs, numIter);

                try (Env<ByteBuffer> env = Env.create()
                        .setMapSize(numIter * 4)
                        .setMaxDbs(1)
                        .open(new File("tests/lmdb_" + numIter))) {

                    startTimeNs = System.nanoTime();
                    Dbi<ByteBuffer> dbi = env.openDbi("lmdb_db", MDB_CREATE);
                    for (int i = 0; i < numIter; i++) {
                        ByteBuffer key = ByteBuffer.allocateDirect(env.getMaxKeySize());
                        key.put(("user" + i).getBytes()).flip();
                        ByteBuffer val = ByteBuffer.allocateDirect(4);
                        val.putInt(i).flip();
                        dbi.put(key, val);
                    }
                    durationNs = System.nanoTime() - startTimeNs;
                    dbi.close();
                }
                results[4] = new BenchmarkResult("LMDB", durationNs, numIter);


                // Results
                Arrays.sort(results, Comparator.comparingLong(BenchmarkResult::getTimeNs));
                for (int i = 0; i < results.length; i++) {
                    results[i].setRank(i + 1);
                }

                for (BenchmarkResult result : results) {
                    writer.write(String.format("%-12s %-12.2f %-18.2f %-18.2f %-12d\n",
                            result.getDatabase(),
                            result.getTimeMs(),
                            result.getTimePerIterationMs(),
                            result.getTimePerIterationNs(),
                            result.getRank()));
                }
                writer.write("\n");
            }
        }

        System.out.println("Benchmark complete! Results written to 'results/benchmark_results.txt'.");
    }

    private static void warmupNoksDB(NoksDBMap db, int warmup) {
        for (int i = 0; i < warmup; i++) {
            db.createRow("user" + i)
                    .value("balance", i)
                    .insert();
            db.delete("user" + i);
        }
    }

    private static void warmupSQL(PreparedStatement stmt, int warmup) throws SQLException {
        for (int i = 0; i < warmup; i++) {
            stmt.setInt(1, i);
            stmt.addBatch();
        }
        stmt.clearBatch();
    }

    private static void warmupRocksDB(RocksDB db, int warmup) throws RocksDBException {
        for (int i = 0; i < warmup; i++) {
            db.put(("user" + i).getBytes(), String.valueOf(i).getBytes());
            db.delete(("user" + i).getBytes());
        }
    }

    static class BenchmarkResult {
        private final String database;
        private final long timeNs;
        private final int numIterations;
        private int rank;

        public BenchmarkResult(String database, long timeNs, int numIterations) {
            this.database = database;
            this.timeNs = timeNs;
            this.numIterations = numIterations;
        }

        public String getDatabase() {
            return database;
        }

        public long getTimeNs() {
            return timeNs;
        }

        public double getTimeMs() {
            return timeNs / 1_000_000.0;
        }

        public double getTimePerIterationNs() {
            return timeNs / (double) numIterations;
        }

        public double getTimePerIterationMs() {
            return getTimePerIterationNs() / 1_000_000.0;
        }

        public int getRank() {
            return rank;
        }

        public void setRank(int rank) {
            this.rank = rank;
        }
    }

    */

    public static void main(String[] args) throws Exception {
        int[] numIterations = {
                1, 3, 7, 10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 350, 400, 450, 500,
                550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 2000, 3000, 4000, 5000,
                6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000,
                80000, 90000, 100000, 150000, 200000, 250000, 300000, 350000, 400000,
                450000, 500000, 550000, 600000, 650000, 700000, 750000, 800000, 850000,
                900000, 950000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000,
                7000000, 8000000, 9000000, 10000000
        };
        int warmup = 100;

        File resultsFile = new File("results\\benchmark_results.txt");
        if (resultsFile.exists()) resultsFile.delete();
        RocksDB.loadLibrary();
        new File("tests").mkdirs();
        new File("results").mkdirs();

        try (var writer = new FileWriter(resultsFile, true)) {
            writer.write("Benchmarking Results (NoksDB):\n\n");

            for (int numIter : numIterations) {
                writer.write("Iterations: " + numIter + "\n");
                writer.write(String.format("%-12s %-12s %-18s %-18s %-12s\n",
                        "Database", "Time (ms)", "Avg/Iter (ms)", "Avg/Iter (ns)", "Rank"));

                BenchmarkResult[] results = new BenchmarkResult[1];

                // NoksDBMap
                NoksDBMap db = NoksDBMap.builder()
                        .storageFile(new File("tests\\noksdb_" + numIter + ".dat"))
                        .autoSave(false)
                        .build();
                warmupNoksDB(db, warmup);

                long startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    db.createRow("user" + i)
                            .value("balance", i)
                            .insert();
                }
                long durationNs = System.nanoTime() - startTimeNs;
                db.close();
                results[0] = new BenchmarkResult("NoksDBMap", durationNs, numIter);

                /*
                // SQLite
                Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite_" + numIter + ".db");
                PreparedStatement sqliteStmt = sqliteConn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
                sqliteStmt.execute();
                sqliteStmt = sqliteConn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
                warmupSQL(sqliteStmt, warmup);

                sqliteConn.setAutoCommit(false);
                startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    sqliteStmt.setInt(1, i);
                    sqliteStmt.addBatch();
                }
                sqliteStmt.executeBatch();
                durationNs = System.nanoTime() - startTimeNs;
                sqliteConn.rollback();
                sqliteConn.close();
                results[1] = new BenchmarkResult("SQLite", durationNs, numIter);

                // H2
                Connection h2Conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDBMap\\tests\\h2_" + numIter);
                PreparedStatement h2Stmt = h2Conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
                h2Stmt.execute();
                h2Stmt = h2Conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");
                warmupSQL(h2Stmt, warmup);

                h2Conn.setAutoCommit(false);
                startTimeNs = System.nanoTime();
                for (int i = 0; i < numIter; i++) {
                    h2Stmt.setInt(1, i);
                    h2Stmt.addBatch();
                }
                h2Stmt.executeBatch();
                durationNs = System.nanoTime() - startTimeNs;
                h2Conn.rollback();
                h2Conn.close();
                results[2] = new BenchmarkResult("H2", durationNs, numIter);

                // RocksDB
                Options options = new Options()
                        .optimizeLevelStyleCompaction()
                        .setIncreaseParallelism(8)
                        .setCompressionType(CompressionType.NO_COMPRESSION)
                        .setCreateIfMissing(true);
                try (RocksDB rocksDB = RocksDB.open(options, "tests/rocksdb_" + numIter)) {
                    warmupRocksDB(rocksDB, warmup);

                    startTimeNs = System.nanoTime();
                    WriteBatch write = new WriteBatch();
                    for (int i = 0; i < numIter; i++) {
                        write.put(("user" + i).getBytes(), String.valueOf(i).getBytes());
                    }
                    WriteOptions writeOptions = new WriteOptions().setDisableWAL(true).setNoSlowdown(true);
                    rocksDB.write(writeOptions, write);
                    durationNs = System.nanoTime() - startTimeNs;
                    writeOptions.close();
                }
                results[3] = new BenchmarkResult("RocksDB", durationNs, numIter);
                */

                /*
                try (Env<ByteBuffer> env = Env.create()
                        .setMapSize(700)
                        .setMaxDbs(1)
                        .open(new File("tests"))) {

                    startTimeNs = System.nanoTime();
                    Dbi<ByteBuffer> dbi = env.openDbi("lmdb_db");
                    for (int i = 0; i < numIter; i++) {
                        ByteBuffer key = ByteBuffer.allocateDirect(env.getMaxKeySize());
                        key.put(("user" + i).getBytes()).flip();
                        ByteBuffer val = ByteBuffer.allocateDirect(4);
                        val.putInt(i).flip();
                        dbi.put(key, val);
                    }
                    durationNs = System.nanoTime() - startTimeNs;
                    dbi.close();
                }
                results[4] = new BenchmarkResult("LMDB", durationNs, numIter);
                */


                // Results
                Arrays.sort(results, Comparator.comparingLong(BenchmarkResult::getTimeNs));
                for (int i = 0; i < results.length; i++) {
                    results[i].setRank(i + 1);
                }

                for (BenchmarkResult result : results) {
                    writer.write(String.format("%-12s %-12.2f %-18.2f %-18.2f %-12d\n",
                            result.getDatabase(),
                            result.getTimeMs(),
                            result.getTimePerIterationMs(),
                            result.getTimePerIterationNs(),
                            result.getRank()));
                }
                writer.write("\n");
            }
        }

        System.out.println("Benchmark complete! Results written to 'results/benchmark_results.txt'.");
    }

    private static void warmupNoksDB(NoksDBMap db, int warmup) {
        for (int i = 0; i < warmup; i++) {
            db.createRow("user" + i)
                    .value("balance", i)
                    .insert();
            db.delete("user" + i);
        }
    }

    private static void warmupSQL(PreparedStatement stmt, int warmup) throws SQLException {
        for (int i = 0; i < warmup; i++) {
            stmt.setInt(1, i);
            stmt.addBatch();
        }
        stmt.clearBatch();
    }

    private static void warmupRocksDB(RocksDB db, int warmup) throws RocksDBException {
        for (int i = 0; i < warmup; i++) {
            db.put(("user" + i).getBytes(), String.valueOf(i).getBytes());
            db.delete(("user" + i).getBytes());
        }
    }

    static class BenchmarkResult {
        private final String database;
        private final long timeNs;
        private final int numIterations;
        private int rank;

        public BenchmarkResult(String database, long timeNs, int numIterations) {
            this.database = database;
            this.timeNs = timeNs;
            this.numIterations = numIterations;
        }

        public String getDatabase() {
            return database;
        }

        public long getTimeNs() {
            return timeNs;
        }

        public double getTimeMs() {
            return timeNs / 1_000_000.0;
        }

        public double getTimePerIterationNs() {
            return timeNs / (double) numIterations;
        }

        public double getTimePerIterationMs() {
            return getTimePerIterationNs() / 1_000_000.0;
        }

        public int getRank() {
            return rank;
        }

        public void setRank(int rank) {
            this.rank = rank;
        }
    }
}