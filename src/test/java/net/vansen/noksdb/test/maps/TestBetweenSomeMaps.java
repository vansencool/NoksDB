package net.vansen.noksdb.test.maps;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class TestBetweenSomeMaps {

    private static final int NUM_TESTS = 10000;
    private static final int NUM_ITERATIONS = 100000;

    public static void main(String[] args) {
        long[] hashMapPutTimes = new long[NUM_TESTS];
        long[] hashMapGetTimes = new long[NUM_TESTS];
        long[] linkedHashMapPutTimes = new long[NUM_TESTS];
        long[] linkedHashMapGetTimes = new long[NUM_TESTS];
        long[] concurrentHashMapPutTimes = new long[NUM_TESTS];
        long[] concurrentHashMapGetTimes = new long[NUM_TESTS];
        for (int i = 0; i < NUM_TESTS; i++) {
            long nano;

            Map<String, String> hashMap = new HashMap<>();
            nano = System.nanoTime();
            Stream.of("sffss", "sffs")
                    .parallel()
                    .forEach(s -> hashMap.put(s, s));
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                hashMap.put("test" + j, "test" + j);
            }
            hashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                hashMap.get("test" + j);
            }
            hashMapGetTimes[i] = System.nanoTime() - nano;

            Map<String, String> linkedHashMap = new LinkedHashMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                linkedHashMap.put("test" + j, "test" + j);
            }
            linkedHashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                linkedHashMap.get("test" + j);
            }
            linkedHashMapGetTimes[i] = System.nanoTime() - nano;

            Map<String, String> concurrentHashMap = new ConcurrentHashMap<>();
            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentHashMap.put("test" + j, "test" + j);
            }
            concurrentHashMapPutTimes[i] = System.nanoTime() - nano;

            nano = System.nanoTime();
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                concurrentHashMap.get("test" + j);
            }
            concurrentHashMapGetTimes[i] = System.nanoTime() - nano;
        }

        TestBetweenManyMaps.BenchmarkResult[] putResults = {
                calculateResults("HashMap", hashMapPutTimes),
                calculateResults("LinkedHashMap", linkedHashMapPutTimes),
                calculateResults("ConcurrentHashMap", concurrentHashMapPutTimes)
        };

        TestBetweenManyMaps.BenchmarkResult[] getResults = {
                calculateResults("HashMap", hashMapGetTimes),
                calculateResults("LinkedHashMap", linkedHashMapGetTimes),
                calculateResults("ConcurrentHashMap", concurrentHashMapGetTimes)
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

    private static TestBetweenManyMaps.BenchmarkResult calculateResults(String mapName, long[] times) {
        long totalNs = 0;
        for (long time : times) {
            totalNs += time;
        }
        long averageNs = totalNs / times.length;
        double averageMs = averageNs / 1_000_000.0;
        double timePerIterationNs = averageNs / (double) NUM_ITERATIONS;
        double timePerIterationMs = timePerIterationNs / 1_000_000.0;

        return new TestBetweenManyMaps.BenchmarkResult(mapName, averageMs, averageNs, timePerIterationMs, timePerIterationNs);
    }

    private static void printResult(TestBetweenManyMaps.BenchmarkResult result) {
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
