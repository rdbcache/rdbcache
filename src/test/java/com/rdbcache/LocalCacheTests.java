package com.rdbcache;

import com.rdbcache.services.LocalCache;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class LocalCacheTests {

    static private LocalCache localCache;

    @BeforeAll
    static void initAll() {
        localCache = new LocalCache();
        localCache.setRecycleMills(150L);
        localCache.start();
    }

    @BeforeEach
    void init() {
    }

    @Test
    void timeToLiveTest() {
        localCache.put("key1", "value1", 900L);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        String value = (String) localCache.get("key1");
        assertEquals(value, "value1");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        value = (String) localCache.get("key1");
        assertNull(value);
    }

    @Test
    void refreshableTest() {
        long start = System.currentTimeMillis();
        localCache.put("key", 100L, () -> {
            return Long.valueOf((System.currentTimeMillis() - start) / 100);
        });
        for (int i = 0; i < 10; i++) {
            Long lvalue = (Long) localCache.get("key");
            assertEquals(lvalue.longValue(), i);
            try {
                Thread.sleep(101);
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    void failingTest() {
        //fail("a failing test");
    }

    @Test
    @Disabled("for demonstration purposes")
    void skippedTest() {
        // not executed
    }

    @AfterEach
    void tearDown() {
    }

    @AfterAll
    static void tearDownAll() {
    }

}
