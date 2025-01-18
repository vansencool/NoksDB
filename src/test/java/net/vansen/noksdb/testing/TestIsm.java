package net.vansen.noksdb.testing;

import net.vansen.noksdb.maps.NoksMap;

import java.util.concurrent.ConcurrentHashMap;

public class TestIsm {

    @SuppressWarnings("all")
    public static void main(String[] args) throws InterruptedException {
        int[] numEntriesValues = {1, 10, 100, 1000, 10000};
        int numRuns = 100;

        for (int numEntries : numEntriesValues) {
            long[] noksMapPutTimes = new long[numRuns];
            long[] concurrentHashMapPutTimes = new long[numRuns];

            long[] noksMapGetTimes = new long[numRuns];
            long[] concurrentHashMapGetTimes = new long[numRuns];

            for (int i = 0; i < numRuns; i++) {
                NoksMap<String, Integer> noksMap = new NoksMap<>();
                ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();

                long startTime = System.nanoTime();
                for (int k = 0; k < numEntries; k++) {
                    noksMap.put("key" + k, k);
                }
                long endTime = System.nanoTime();
                noksMapPutTimes[i] = endTime - startTime;

                startTime = System.nanoTime();
                for (int k = 0; k < numEntries; k++) {
                    concurrentHashMap.put("key" + k, k);
                }
                endTime = System.nanoTime();
                concurrentHashMapPutTimes[i] = endTime - startTime;

                startTime = System.nanoTime();
                for (int k = 0; k < numEntries; k++) {
                    noksMap.get("key" + k);
                }
                endTime = System.nanoTime();
                noksMapGetTimes[i] = endTime - startTime;

                startTime = System.nanoTime();
                for (int k = 0; k < numEntries; k++) {
                    concurrentHashMap.get("key" + k);
                }
                endTime = System.nanoTime();
                concurrentHashMapGetTimes[i] = endTime - startTime;
            }

            long noksMapPutAverageTime = calculateAverage(noksMapPutTimes);
            long concurrentHashMapPutAverageTime = calculateAverage(concurrentHashMapPutTimes);

            long noksMapGetAverageTime = calculateAverage(noksMapGetTimes);
            long concurrentHashMapGetAverageTime = calculateAverage(concurrentHashMapGetTimes);

            System.out.println("Num Entries: " + numEntries);
            System.out.println("NoksMap put:");
            System.out.println("  Average time: " + noksMapPutAverageTime + " ns");
            System.out.println("  Average time per entry: " + (noksMapPutAverageTime / (double) numEntries) + " ns");
            System.out.println("  Average time per entry (ms): " + ((noksMapPutAverageTime / (double) numEntries) / 1e6) + " ms");
            System.out.println("  Total time: " + (noksMapPutAverageTime / 1e9) + " seconds");
            System.out.println();

            System.out.println("ConcurrentHashMap put:");
            System.out.println("  Average time: " + concurrentHashMapPutAverageTime + " ns");
            System.out.println("  Average time per entry: " + (concurrentHashMapPutAverageTime / (double) numEntries) + " ns");
            System.out.println("  Average time per entry (ms): " + ((concurrentHashMapPutAverageTime / (double) numEntries) / 1e6) + " ms");
            System.out.println("  Total time: " + (concurrentHashMapPutAverageTime / 1e9) + " seconds");
            System.out.println();
            System.out.println("NoksMap get:");
            System.out.println("  Average time: " + noksMapGetAverageTime + " ns");
            System.out.println("  Average time per entry: " + (noksMapGetAverageTime / (double) numEntries) + " ns");
            System.out.println("  Average time per entry (ms): " + ((noksMapGetAverageTime / (double) numEntries) / 1e6) + " ms");
            System.out.println("  Total time: " + (noksMapGetAverageTime / 1e9) + " seconds");
            System.out.println();

            System.out.println("ConcurrentHashMap get:");
            System.out.println("  Average time: " + concurrentHashMapGetAverageTime + " ns");
            System.out.println("  Average time per entry: " + (concurrentHashMapGetAverageTime / (double) numEntries) + " ns");
            System.out.println("  Average time per entry (ms): " + ((concurrentHashMapGetAverageTime / (double) numEntries) / 1e6) + " ms");
            System.out.println("  Total time: " + (concurrentHashMapGetAverageTime / 1e9) + " seconds");
            System.out.println();
        }

        NoksMap<String, String> map = new NoksMap<>();

        // Create one thread that puts a key-value pair
        Thread putThread = new Thread(() -> {
            map.put("key", "value");
            System.out.println("Put thread put key-value pair");
        });

        // Create another thread that removes the same key
        Thread removeThread = new Thread(() -> {
            map.remove("key");
            System.out.println("Remove thread removed key-value pair");
        });

        // Create another thread that tries to get the same key
        Thread getThread = new Thread(() -> {
            String value = map.get("key");
            System.out.println("Get thread got value: " + value);
        });

        putThread.start();
        putThread.join(); // Wait for putThread to finish

        removeThread.start();
        removeThread.join(); // Wait for removeThread to finish

        getThread.start();
        getThread.join(); // Wait for getThread to finish
    }

    private static long calculateAverage(long[] times) {
        long sum = 0;
        for (long time : times) {
            sum += time;
        }
        return sum / times.length;
    }
}
