package net.vansen.noksdb.testing.maps;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class TestBetweenManyMaps {

    private static final int NUM_TESTS = 1000;
    private static final int NUM_ITERATIONS = 100;

    public static void main(String[] args) {
        long[] hashMapPutTimes = new long[NUM_TESTS];
        long[] hashMapGetTimes = new long[NUM_TESTS];
        long[] linkedHashMapPutTimes = new long[NUM_TESTS];
        long[] linkedHashMapGetTimes = new long[NUM_TESTS];
        long[] concurrentHashMapPutTimes = new long[NUM_TESTS];
        long[] concurrentHashMapGetTimes = new long[NUM_TESTS];
        /*long[] treeMapPutTimes = new long[NUM_TESTS];
        long[] treeMapGetTimes = new long[NUM_TESTS];
        long[] concurrentSkipListMapPutTimes = new long[NUM_TESTS];
        long[] concurrentSkipListMapGetTimes = new long[NUM_TESTS];
         */
        for (int i = 0; i < NUM_TESTS; i++) {
            long nano;

            // HashMap
            Map<String, String> hashMap = new HashMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                hashMap.put("testing" + j, "testing" + j);
            }
            hashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                hashMap.get("testing" + j);
            }
            hashMapGetTimes[i] = System.nanoTime() - nano;

            // LinkedHashMap
            Map<String, String> linkedHashMap = new LinkedHashMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                linkedHashMap.put("testing" + j, "testing" + j);
            }
            linkedHashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                linkedHashMap.get("testing" + j);
            }
            linkedHashMapGetTimes[i] = System.nanoTime() - nano;

            // ConcurrentHashMap
            Map<String, String> concurrentHashMap = new ConcurrentHashMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentHashMap.put("testing" + j, "testing" + j);
            }
            concurrentHashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentHashMap.get("testing" + j);
            }
            concurrentHashMapGetTimes[i] = System.nanoTime() - nano;

            /*
            // TreeMap
            Map<String, String> treeMap = new TreeMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                treeMap.put("testing" + j, "testing" + j);
            }
            treeMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                treeMap.get("testing" + j);
            }
            treeMapGetTimes[i] = System.nanoTime() - nano;

            // ConcurrentSkipListMap
            Map<String, String> concurrentSkipListMap = new ConcurrentSkipListMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentSkipListMap.put("testing" + j, "testing" + j);
            }
            concurrentSkipListMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentSkipListMap.get("testing" + j);
            }
            concurrentSkipListMapGetTimes[i] = System.nanoTime() - nano;
             */
        }

        // Calculate results
        BenchmarkResult[] putResults = {
                calculateResults("HashMap", hashMapPutTimes),
                calculateResults("LinkedHashMap", linkedHashMapPutTimes),
                calculateResults("ConcurrentHashMap", concurrentHashMapPutTimes)/*,
                calculateResults("TreeMap", treeMapPutTimes),
                calculateResults("ConcurrentSkipListMap", concurrentSkipListMapPutTimes),*/
        };

        BenchmarkResult[] getResults = {
                calculateResults("HashMap", hashMapGetTimes),
                calculateResults("LinkedHashMap", linkedHashMapGetTimes),
                calculateResults("ConcurrentHashMap", concurrentHashMapGetTimes)/*,
                calculateResults("TreeMap", treeMapGetTimes),
                calculateResults("ConcurrentSkipListMap", concurrentSkipListMapGetTimes),*/
        };

        Arrays.sort(putResults, Comparator.comparingDouble(r -> r.averageMs));
        Arrays.sort(getResults, Comparator.comparingDouble(r -> r.averageMs));

        System.out.println("PUT Benchmark Results:");
        System.out.printf("%-20s %-12s %-12s %-18s %-18s\n", "Map Type", "Avg Time (ms)", "Avg Time (ns)", "Time/Iter (ms)", "Time/Iter (ns)");
        System.out.println("=".repeat(80));
        for (int i = 0; i < putResults.length; i++) {
            putResults[i].rank = i + 1;
            printResult(putResults[i]);
        }

        System.out.println("\nGET Benchmark Results:");
        System.out.printf("%-20s %-12s %-12s %-18s %-18s\n", "Map Type", "Avg Time (ms)", "Avg Time (ns)", "Time/Iter (ms)", "Time/Iter (ns)");
        System.out.println("=".repeat(80));
        for (int i = 0; i < getResults.length; i++) {
            getResults[i].rank = i + 1;
            printResult(getResults[i]);
        }
    }

    private static BenchmarkResult calculateResults(String mapName, long[] times) {
        long totalNs = 0;
        for (long time : times) {
            totalNs += time;
        }
        long averageNs = totalNs / times.length;
        double averageMs = averageNs / 1_000_000.0;
        double timePerIterationNs = averageNs / (double) NUM_ITERATIONS;
        double timePerIterationMs = timePerIterationNs / 1_000_000.0;

        return new BenchmarkResult(mapName, averageMs, averageNs, timePerIterationMs, timePerIterationNs);
    }

    private static void printResult(BenchmarkResult result) {
        System.out.printf("%-20s %-12.2f %-12d %-18.6f %-18.2f\n",
                result.mapName,
                result.averageMs,
                result.averageNs,
                result.timePerIterationMs,
                result.timePerIterationNs);
    }

    static class BenchmarkResult {
        String mapName;
        double averageMs;
        long averageNs;
        double timePerIterationMs;
        double timePerIterationNs;
        int rank;

        public BenchmarkResult(String mapName, double averageMs, long averageNs, double timePerIterationMs, double timePerIterationNs) {
            this.mapName = mapName;
            this.averageMs = averageMs;
            this.averageNs = averageNs;
            this.timePerIterationMs = timePerIterationMs;
            this.timePerIterationNs = timePerIterationNs;
        }
    }
}