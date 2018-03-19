package com.rdbcache.helpers;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.models.KeyInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

public class AnyKeyTest {

    @Test
    public void setKey() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();
        keyInfo = new KeyInfo("100", "table");
        anyKey.setKey(keyInfo);

        assertEquals(1, anyKey.size());
        assertTrue(keyInfo == anyKey.get(0));

        anyKey = new AnyKey(new KeyInfo());
        anyKey.setKey(keyInfo);
        assertEquals(1, anyKey.size());
        assertTrue(keyInfo == anyKey.get(0));
    }

    @Test
    public void getKey() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();

        keyInfo = anyKey.getKey();
        assertNull(keyInfo);

        keyInfo = new KeyInfo("100", "table");
        anyKey = new AnyKey(keyInfo);

        KeyInfo keyInfo2 = anyKey.getKey();

        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);
    }

    @Test
    public void getAny() {

        AnyKey anyKey;
        KeyInfo keyInfo;

        anyKey = new AnyKey();

        keyInfo = anyKey.getAny();
        assertNotNull(keyInfo);

        keyInfo = new KeyInfo("100", "table");
        anyKey = new AnyKey(keyInfo);
        KeyInfo keyInfo2 = anyKey.getAny();

        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);

        keyInfo2 = anyKey.get(0);
        assertNotNull(keyInfo2);
        assertTrue(keyInfo == keyInfo2);

        for (int i = 0; i < 10; i++) {
            keyInfo = anyKey.getAny(i);
            assertNotNull(keyInfo);
            if (i == 0) assertFalse(keyInfo.isNew());
            else assertTrue(keyInfo.isNew());
            assertEquals(i+1, anyKey.size());
        }
    }

}