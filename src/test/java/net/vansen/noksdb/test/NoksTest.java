package net.vansen.noksdb.test;

import it.unimi.dsi.fastutil.objects.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NoksTest {

    public static void main(String[] args) throws IOException, SQLException {
        /*
        NoksDBMap.Table users = db.table("users");

        for (int i = 0; i < 100000; i++) {
            users.createRow("user" + i)
                    .value("username", "user" + i)
                    .value("password", "secret" + i)
                    .value("balance", i)
                    .insert();
    }
    */

    /*
        long startTime = System.nanoTime();
        Object user = users.fetch()
                .where("username", "user99999")
                .where("password", "secret99999")
                .key("balance")
                .get();
        long endTime = System.nanoTime();

        System.out.println("Fetched user: " + user);
        System.out.println("Fetch time: " + (endTime - startTime) + " nanoseconds");
        System.out.println("Fetch time: " + (endTime - startTime) / 1e6 + " milliseconds");

        db.save();
        */

        /*
        NoksDBMap db = NoksDBMap.builder()
                .storageFile(new File("tests\\noksdb compressed 1000000.dat"))
                .autoSave(false)
                .compression(new LZFCompression())
                .build();

        System.out.println(db.fetch().where("user50").key("balance").get());
        */

        /*
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
            long startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                db.createRow("user" + i)
                        .value("balance", i)
                        .insert();
            }
            long endTimeNs = System.nanoTime();
            long endTimeMs = System.currentTimeMillis();
            long durationNs = endTimeNs - startTimeNs;
            long durationMs = endTimeMs - startTimeMs;
            System.out.println("NoksDBMap:");
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
            db.save();
            db.close();

            Connection conn = DriverManager.getConnection("jdbc:sqlite:tests\\sqlite " + numIter + ".db");
            PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();
            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            conn.setAutoCommit(false); // Start a transaction

            for (int i = 0; i < warmup; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.clearBatch();

            startTimeNs = System.nanoTime();
            startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.executeBatch();
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
            conn.commit();
            conn.close();

            // H2
            conn = DriverManager.getConnection("jdbc:h2:file:D:\\minecraft\\Projects\\NoksDBMap\\tests\\h2 " + numIter + ".db");
            stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS users (balance INTEGER)");
            stmt.execute();

            conn.setAutoCommit(false); // Start a transaction

            stmt = conn.prepareStatement("INSERT INTO users (balance) VALUES (?)");

            for (int i = 0; i < warmup; i++) {
                stmt.setInt(1, i);
                stmt.addBatch();
            }
            stmt.clearBatch();

            startTimeNs = System.nanoTime();
            startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                stmt.setInt(1, i);
                stmt.addBatch(); // Add to the batch
            }
            stmt.executeBatch(); // Execute the batch
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
            conn.commit(); // Commit the transaction
            conn.close();
            System.out.println("-".repeat(50));
            System.out.println();

        }
        */

        /*
        System.out.println("Get performance test:");
        for (int numIter : numIterations) {
            long startTimeNs = System.nanoTime();
            long startTimeMs = System.currentTimeMillis();
            for (int i = 0; i < numIter; i++) {
                db.table("users")
                        .fetch()
                        .key("username" + i)
                        .get();
            }
            long endTimeNs = System.nanoTime();
            long endTimeMs = System.currentTimeMillis();
            long durationNs = endTimeNs - startTimeNs;
            long durationMs = endTimeMs - startTimeMs;
            System.out.println("Num iterations: " + numIter);
            System.out.println("Time taken (ns): " + durationNs);
            System.out.println("Time taken (ms): " + durationMs);
            System.out.println("Average time per iteration (ns): " + (durationNs / (double) numIter));
            System.out.println("Average time per iteration (ms): " + (durationMs / (double) numIter));
            System.out.println();
        }
        */
        testConcurrentHashMap();
    }

    public static void testConcurrentHashMap() {
        int numElements = 100000;
        int numThreads = 2;
        ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();

        long startTime = System.nanoTime();
        populateMap(concurrentHashMap, numElements, numThreads);
        long endTime = System.nanoTime();
        System.out.println("-".repeat(100));
        System.out.println("ConcurrentHashMap: " + (endTime - startTime) / 1e6 + " ms");
        System.out.println("-".repeat(100));
    }

    public static void testObject2ObjectArrayMap() {
        int numElements = 100000;
        int numThreads = 4;
        Object2ObjectArrayMap<String, Integer> object2ObjectArrayMap = new Object2ObjectArrayMap<>();
        populateMap(object2ObjectArrayMap, 4, numElements);

        long startTime = System.nanoTime();
        AtomicInteger sum = new AtomicInteger();
        testMap(object2ObjectArrayMap, sum, numThreads, numElements);
        long endTime = System.nanoTime();
        System.out.println("Object2ObjectArrayMap: " + (endTime - startTime) / 1e6 + " ms");
    }

    private static void populateMap(Map<String, Integer> map, int numElements, int numThreads) {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        int chunkSize = numElements / numThreads;

        List<CompletableFuture<?>> futures = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = (i == numThreads - 1) ? numElements : (i + 1) * chunkSize;

            Runnable task = new MapPopulateTask(map, start, end);
            CompletableFuture<?> future = CompletableFuture.runAsync(task, executor);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        executor.shutdown();
    }

    private static class MapPopulateTask implements Runnable {
        private Map<String, Integer> map;
        private int start;
        private int end;

        public MapPopulateTask(Map<String, Integer> map, int start, int end) {
            this.map = map;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            for (int i = start; i < end; i++) {
                map.put(String.valueOf(i), i);
            }
        }
    }

    private static void testMap(Map<String, Integer> map, AtomicInteger sum, int numThreads, int numElements) {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        int chunkSize = numElements / numThreads;

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            int start = i * chunkSize;
            int end = (i == numThreads - 1) ? numElements : (i + 1) * chunkSize;

            Runnable task = new MapTask(map, sum, start, end);
            CompletableFuture<?> future = CompletableFuture.runAsync(task, executor);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        executor.shutdown();
    }

    private static class MapTask implements Runnable {
        private Map<String, Integer> map;
        private AtomicInteger sum;
        private int start;
        private int end;

        public MapTask(Map<String, Integer> map, AtomicInteger sum, int start, int end) {
            this.map = map;
            this.sum = sum;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            for (int j = start; j < end; j++) {
                System.out.println(map.get(String.valueOf(j)));
            }
        }
    }

    @Test
    public static void testObjectArrayList() {
        int numElements = 100000;
        ObjectArrayList<Integer> objectArrayList = new ObjectArrayList<>();
        populateList(objectArrayList, numElements);

        long startTime = System.nanoTime();
        for (int i = 0; i < numElements; i++) {
            objectArrayList.get(i);
        }
        long endTime = System.nanoTime();
        System.out.println("ObjectArrayList: " + (endTime - startTime) / 1e6 + " ms");
    }

    @Test
    public static void testList() {
        int numElements = 100000;
        List<Integer> objectArrayList = new ArrayList<>();
        populateList(objectArrayList, numElements);

        long startTime = System.nanoTime();
        int sum = 0;
        for (int i = 0; i < numElements; i++) {
            sum += objectArrayList.get(i);
        }
        long endTime = System.nanoTime();
        System.out.println("Normal list: " + (endTime - startTime) / 1e6 + " ms");
    }

    @Test
    public static void testObjectBigArrayBigList() {
        int numElements = 100000;
        ObjectBigArrayBigList<Integer> objectBigArrayBigList = new ObjectBigArrayBigList<>();
        populateList(objectBigArrayBigList, numElements);

        long startTime = System.nanoTime();
        int sum = 0;
        for (int i = 0; i < numElements; i++) {
            sum += objectBigArrayBigList.get(i);
        }
        long endTime = System.nanoTime();
        System.out.println("ObjectBigArrayBigList: " + (endTime - startTime) / 1e6 + " ms");
    }

    private static void populateList(ObjectList<Integer> list, int numElements) {
        for (int i = 0; i < numElements; i++) {
            list.add(i);
        }
    }

    private static void populateList(ObjectBigList<Integer> list, int numElements) {
        for (int i = 0; i < numElements; i++) {
            list.add(i);
        }
    }

    private static void populateList(List<Integer> list, int numElements) {
        for (int i = 0; i < numElements; i++) {
            list.add(i);
        }
    }

    /*
    @Test
    public static void testPutAndGet() {
        File dbFile = new File("noksdb.dat");
        NoksDBMap db = new NoksDBMap.Builder()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key = "testKey";
        String value = "testValue";

        db.put(key, value);

        String retrievedValue = db.get(key);

        System.out.println("Test Put and Get:");
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        System.out.println("Retrieved Value: " + retrievedValue);
        System.out.println("Result: " + (retrievedValue.equals(value) ? "PASS" : "FAIL"));
    }

    @Test
    public static void testSaveAndLoad() {
        File dbFile = new File("noksdb.dat");
        NoksDBMap<String> db = new NoksDBMap.Builder<String>()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key = "testKey";
        String value = "testValue";

        db.put(key, value);

        db.save();

        NoksDBMap<String> loadedDb = new NoksDBMap.Builder<String>()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String retrievedValue = loadedDb.get(key);

        System.out.println("Test Save and Load:");
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        System.out.println("Retrieved Value: " + retrievedValue);
        System.out.println("Result: " + (retrievedValue.equals(value) ? "PASS" : "FAIL"));
    }

    @Test
    public static void testPutAndGetDifferentObjects() {
        File dbFile = new File("noksdb.dat");
        NoksDBMap<String> db = new NoksDBMap.Builder<String>()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key1 = "testKey1";
        String value1 = "testValue1";

        String key2 = "testKey2";
        String value2 = "testValue2";

        db.put(key1, value1);
        db.put(key2, value2);

        String retrievedValue1 = db.get(key1);
        String retrievedValue2 = db.get(key2);

        System.out.println("Test Put and Get Different Objects:");
        System.out.println("Key 1: " + key1);
        System.out.println("Value 1: " + value1);
        System.out.println("Retrieved Value 1: " + retrievedValue1);
        System.out.println("Result 1: " + (retrievedValue1.equals(value1) ? "PASS" : "FAIL"));
        System.out.println("Key 2: " + key2);
        System.out.println("Value 2: " + value2);
        System.out.println("Retrieved Value 2: " + retrievedValue2);
        System.out.println("Result 2: " + (retrievedValue2.equals(value2) ? "PASS" : "FAIL"));
    }

     */
}