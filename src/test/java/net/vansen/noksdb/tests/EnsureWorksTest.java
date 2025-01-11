package net.vansen.noksdb.tests;

import net.vansen.noksdb.NoksDB;
import org.junit.jupiter.api.Test;

import java.io.File;

public class EnsureWorksTest {

    public static void main(String[] args) {
        testPutAndGet();
        testSaveAndLoad();
        testPutAndGetDifferentObjects();
    }

    @Test
    public static void testPutAndGet() {
        File dbFile = new File("noksdb.dat");
        NoksDB db = new NoksDB.Builder()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key = "testKey";
        String value = "testValue";

        db.rowOf(key)
                .value(key, value)
                .insert();

        String retrievedValue = (String) db.fetch(key)
                .field(key)
                .get();

        System.out.println("Test Put and Get:");
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        System.out.println("Retrieved Value: " + retrievedValue);
        System.out.println("Result: " + (retrievedValue.equals(value) ? "PASS" : "FAIL"));
    }

    @Test
    public static void testSaveAndLoad() {
        File dbFile = new File("noksdb.dat");
        NoksDB db = NoksDB.builder()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key = "testKey";
        String value = "testValue";

        db.rowOf(key)
                .value(key, value)
                .insert();

        db.save();

        NoksDB loadedDb = NoksDB.builder()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String retrievedValue = (String) loadedDb.fetch(key)
                .field(key)
                .get();

        System.out.println("Test Save and Load:");
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        System.out.println("Retrieved Value: " + retrievedValue);
        System.out.println("Result: " + (retrievedValue.equals(value) ? "PASS" : "FAIL"));
    }

    @Test
    public static void testPutAndGetDifferentObjects() {
        File dbFile = new File("noksdb.dat");
        NoksDB db = NoksDB.builder()
                .storageFile(dbFile)
                .autoSave(false)
                .build();

        String key1 = "testKey1";
        String value1 = "testValue1";

        String key2 = "testKey2";
        String value2 = "testValue2";

        db.rowOf(key1)
                .value("balance", value1)
                .insert();
        db.rowOf(key2)
                .value("balance", value2)
                .insert();

        String retrievedValue1 = (String) db.fetch(key1)
                .field("balance")
                .get();
        String retrievedValue2 = (String) db.fetch(key2)
                .field("balance")
                .get();

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
}
