package com.rdbcache.helpers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.KeyInfo;

import com.rdbcache.services.LocalCache;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;

public class AnyKeyTest {

    @Test
    public void setKey() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();
        keyInfo = new KeyInfo();
        keyInfo.setExpire("100");
        keyInfo.setTable("table");
        anyKey.setKeyInfo(keyInfo);

        assertEquals(1, anyKey.size());
        assertTrue(keyInfo == anyKey.get(0));

        anyKey = new AnyKey(new KeyInfo());
        anyKey.setKeyInfo(keyInfo);
        assertEquals(1, anyKey.size());
        assertTrue(keyInfo == anyKey.get(0));
    }

    @Test
    public void getKey() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();

        keyInfo = anyKey.getKeyInfo();
        assertNull(keyInfo);

        keyInfo = new KeyInfo();
        keyInfo.setExpire("100");
        keyInfo.setTable("table");
        anyKey = new AnyKey(keyInfo);

        KeyInfo keyInfo2 = anyKey.getKeyInfo();

        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);
    }

    @Test
    public void getAny() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();

        keyInfo = anyKey.getKeyInfo();
        assertNull(keyInfo);

        keyInfo = new KeyInfo();
        keyInfo.setExpire("100");
        keyInfo.setTable("table");
        anyKey = new AnyKey(keyInfo);
        KeyInfo keyInfo2 = anyKey.getKeyInfo();

        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);

        keyInfo2 = anyKey.get(0);
        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);

        LocalCache localCache = new LocalCache();
        localCache.init();
        localCache.handleEvent(null);

        AppCtx.setLocalCache(localCache);

        try {
            for (int i = 0; i < 10; i++) {
                keyInfo = anyKey.getAny(i);
                assertNotNull(keyInfo);
                if (i == 0) assertFalse(keyInfo.getIsNew());
                else assertTrue(keyInfo.getIsNew());
                assertEquals(i + 1, anyKey.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getCause().getMessage());
        }
    }

}