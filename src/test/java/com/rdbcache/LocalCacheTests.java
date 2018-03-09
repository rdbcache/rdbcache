package com.rdbcache;

import com.rdbcache.helpers.Utils;
import com.rdbcache.services.LocalCache;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LocalCacheTests {

    static private LocalCache localCache;

    @BeforeAll
    static void initAll() {
        localCache = new LocalCache();
        localCache.setRecycleSecs(1L);
        localCache.start();
    }

    @BeforeEach
    void init() {
    }

    @Test
    void timeToLiveTest() {
        localCache.put("key", Utils.toMap("{\"k\":\"v\"}"), 900L);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        Map<String, Object> map = localCache.get("key");
        assertEquals(Utils.toJson(map), "{\"k\":\"v\"}");
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        map = localCache.get("key");
        assertNull(map);
    }

    @Test
    void refreshableTest() {
        long start = System.currentTimeMillis();
        localCache.put("key", 1000L, () -> {
            Map<String, Object> map = new HashMap<>();
            map.put("time_lot", Long.valueOf((System.currentTimeMillis() - start) / 1000));
            return map;
        });
        for (Long i = 0L; i < 10L; i++) {
            Map<String, Object> map = localCache.get("key");
            Long timeLot = (Long) map.get("time_lot");
            assertEquals(timeLot, i);
            try {
                Thread.sleep(1001);
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
