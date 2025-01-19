package net.vansen.noksdb.testing;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.bulk.BulkBuilder;
import net.vansen.noksdb.compression.impl.NoCompression;
import net.vansen.noksdb.maps.MapType;
import net.vansen.noksdb.maps.NoksMap;
import oshi.SystemInfo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestIsm {

    @SuppressWarnings("all")
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] numEntriesValues = {1, 10, 100, 1000, 10000, 100000};
        int numRuns = 1000;
        String notes = "################# Notes: NoksDB with HashMap #################";
        String mainNote = "HashMap";

        FileWriter fileWriter = new FileWriter("results\\output-" + mainNote + ".log");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        bufferedWriter.write("");
        bufferedWriter.flush();

        bufferedWriter.write(notes + "\n\n");

        SystemInfo si = new SystemInfo();

        bufferedWriter.write("Information:\n");
        bufferedWriter.write("  Physical Cores: " + si.getHardware().getProcessor().getPhysicalProcessorCount() + "\n");
        bufferedWriter.write("  Logical Cores: " + si.getHardware().getProcessor().getLogicalProcessorCount() + "\n");
        bufferedWriter.write("  Allocated Memory: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB\n");
        bufferedWriter.write("  Max Memory: " + (si.getHardware().getMemory().getTotal() / (1024 * 1024)) + " MB\n");
        bufferedWriter.write("  CPU Model: " + si.getHardware().getProcessor().getProcessorIdentifier().getName() + "\n");
        bufferedWriter.write("  Memory type: " + si.getHardware().getMemory().getPhysicalMemory().getFirst().getMemoryType() + "\n");
        bufferedWriter.write("  Operating System: " + si.getOperatingSystem().getFamily() + "\n");
        bufferedWriter.write("\n\n");

        for (int numEntries : numEntriesValues) {
            long[] noksMapPutTimes = new long[numRuns];
            long[] concurrentHashMapPutTimes = new long[numRuns];

            long[] noksMapGetTimes = new long[numRuns];
            long[] concurrentHashMapGetTimes = new long[numRuns];

            long[] noksDBGetTimes = new long[numRuns];
            long[] noksDBPutTimes = new long[numRuns];

            for (int i = 0; i < numRuns; i++) {
                NoksMap<String, Integer> noksMap = new NoksMap<>();
                ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
                NoksDB db = NoksDB.builder()
                        .autoSave(false)
                        .mapType(MapType.HASH_MAP)
                        .compression(NoCompression.instance())
                        .build();

                warmupMaps(noksMap, concurrentHashMap);
                warmupDB(db);

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

                startTime = System.nanoTime();
                BulkBuilder builder = db.bulk();
                for (int k = 0; k < numEntries; k++) {
                    builder.add("key" + k, Map.of("val", k));
                }
                // builder.executeUnOrdered(); // ConcurrentHashMap
                builder.execute(); // Has to be ordered, otherwise the test will fail - NoksMap
                endTime = System.nanoTime();
                noksDBPutTimes[i] = endTime - startTime;

                startTime = System.nanoTime();
                for (int k = 0; k < numEntries; k++) {
                    db.store().get("key" + k).get("val");
                }
                endTime = System.nanoTime();
                noksDBGetTimes[i] = endTime - startTime;
            }

            long noksMapPutAverageTime = calculateAverage(noksMapPutTimes);
            long concurrentHashMapPutAverageTime = calculateAverage(concurrentHashMapPutTimes);

            long noksMapGetAverageTime = calculateAverage(noksMapGetTimes);
            long concurrentHashMapGetAverageTime = calculateAverage(concurrentHashMapGetTimes);

            long noksDBGetAverageTime = calculateAverage(noksDBGetTimes);
            long noksDBPutAverageTime = calculateAverage(noksDBPutTimes);

            bufferedWriter.write("Num Entries: " + numEntries + "\n");
            bufferedWriter.write("-------------------------------\n");

            bufferedWriter.write("NoksMap put:\n");
            bufferedWriter.write("  Average time: " + noksMapPutAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (noksMapPutAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((noksMapPutAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (noksMapPutAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("ConcurrentHashMap put:\n");
            bufferedWriter.write("  Average time: " + concurrentHashMapPutAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (concurrentHashMapPutAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((concurrentHashMapPutAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (concurrentHashMapPutAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("NoksDB put:\n");
            bufferedWriter.write("  Average time: " + noksDBPutAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (noksDBPutAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((noksDBPutAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (noksDBPutAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("NoksMap get:\n");
            bufferedWriter.write("  Average time: " + noksMapGetAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (noksMapGetAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((noksMapGetAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (noksMapGetAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("ConcurrentHashMap get:\n");
            bufferedWriter.write("  Average time: " + concurrentHashMapGetAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (concurrentHashMapGetAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((concurrentHashMapGetAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (concurrentHashMapGetAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("NoksDB get:\n");
            bufferedWriter.write("  Average time: " + noksDBGetAverageTime + " ns\n");
            bufferedWriter.write("  Average time per entry: " + (noksDBGetAverageTime / (double) numEntries) + " ns\n");
            bufferedWriter.write("  Average time per entry (ms): " + ((noksDBGetAverageTime / (double) numEntries) / 1e6) + " ms\n");
            bufferedWriter.write("  Total time: " + (noksDBGetAverageTime / 1e9) + " seconds\n");
            bufferedWriter.write("\n");

            bufferedWriter.write("-------------------------------\n");
        }
        bufferedWriter.close();
    }

    private static long calculateAverage(long[] times) {
        long sum = 0;
        for (long time : times) {
            sum += time;
        }
        return sum / times.length;
    }

    public static void warmupMaps(Map<String, Integer> noksMap, Map<String, Integer> concurrentHashMap) {
        for (int i = 0; i < 5000; i++) {
            noksMap.put(String.valueOf(i), i);
            concurrentHashMap.put(String.valueOf(i), i);
            noksMap.get(String.valueOf(i));
            concurrentHashMap.get(String.valueOf(i));
            noksMap.remove(String.valueOf(i));
            concurrentHashMap.remove(String.valueOf(i));
        }
    }

    public static void warmupDB(NoksDB noksDB) {
        for (int i = 0; i < 5000; i++) {
            noksDB.rowOf(String.valueOf(i)).value(String.valueOf(i), i).insert();
            noksDB.fetch(String.valueOf(i)).field(String.valueOf(i));
            noksDB.delete(String.valueOf(i));
        }
    }
}
