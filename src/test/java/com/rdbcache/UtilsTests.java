package com.rdbcache;

import com.rdbcache.helpers.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTests {

    @BeforeAll
    static void initAll() {
    }

    @BeforeEach
    void init() {
    }

    @Test
    void succeedingTest() {
    }

    @Test
    void toMapTestWithAssociateArray() {
        String json = "{\"a\":1,\"b\":\"c\"}";
        Map<String, Object> map = Utils.toMap(json);
        Map<String, Object> newMap = new LinkedHashMap<>();
        newMap.put("a", 1);
        newMap.put("b", "c");
        assertEquals(map, newMap);
    }

    @Test
    void toMapTestWithArray() {
        String json = "[\"a\",\"b\",\"c\"]";
        Map<String, Object> map = Utils.toMap(json);
        Map<String, Object> newMap = new LinkedHashMap<>();
        newMap.put("0", "a");
        newMap.put("1", "b");
        newMap.put("2", "c");
        assertEquals(map, newMap);
    }

    @Test
    void toJsonTestWithAssociateArray() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "c");
        String json = Utils.toJson(map);
        assertEquals(json, "{\"a\":1,\"b\":\"c\"}");
    }

    @Test
    void toJsonTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        String json = Utils.toJson(list);
        assertEquals(json, "{\"0\":\"a\",\"1\":\"b\",\"2\":\"c\"}");
    }

    @Test
    void toPrettyJsonTestWithAssociateArray() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "c");
        String json = Utils.toPrettyJson(map).replaceAll("\\s", "");
        assertEquals(json, "{\"a\":1,\"b\":\"c\"}");
    }

    @Test
    void toPrettyJsonTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        String json = Utils.toPrettyJson(list).replaceAll("\\s", "");
        assertEquals(json, "{\"0\":\"a\",\"1\":\"b\",\"2\":\"c\"}");
    }

    @Test
    void generateIdTest() {
        String id = Utils.generateId();
        assertEquals(id.indexOf("-"), -1);
        assertEquals(id.indexOf(" "), -1);
        assertEquals(id.indexOf("\n"), -1);
    }

    @Test
    void isValueEqualsTest() {
        assertTrue(Utils.isValueEquals(null, null));
        assertFalse(Utils.isValueEquals(null, ""));
        assertFalse(Utils.isValueEquals("", null));

        String a = "string a";
        String b = "string b";
        String c = "string a";

        assertTrue(Utils.isValueEquals(a, a));
        assertTrue(Utils.isValueEquals(a, c));
        assertFalse(Utils.isValueEquals(a, b));

        a = "string a ";
        b = " string a";
        assertTrue(Utils.isValueEquals(a, b));

        Boolean boolTrue = true, boolFalse = false;
        assertTrue(Utils.isValueEquals(boolTrue, 1));
        assertTrue(Utils.isValueEquals(boolTrue, "true"));
        assertTrue(Utils.isValueEquals(boolTrue, "TRUE"));
        assertTrue(Utils.isValueEquals(boolFalse, 0));
        assertTrue(Utils.isValueEquals(boolFalse, "false"));
        assertTrue(Utils.isValueEquals(boolFalse, "FALSE"));

        Double doubleA = 3.1415926;
        Float floatA = 3.141592F;
        BigDecimal bdA = new BigDecimal("3.1415926");
        assertTrue(Utils.isValueEquals(doubleA, "3.1415926"));
        assertTrue(Utils.isValueEquals(floatA, "3.141592"));
        assertTrue(Utils.isValueEquals(bdA, "3.1415926"));

        Long longA = 31415926L;
        Integer integerA = 3141592;
        assertTrue(Utils.isValueEquals(longA, "31415926"));
        assertTrue(Utils.isValueEquals(integerA, "3141592"));
    }

    @Test
    void mapChangesAfterUpdateTest() {

        Map<String, Object> changes = new LinkedHashMap<>();
        assertFalse(Utils.mapChangesAfterUpdate(null, null, changes));

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", "C");
        map.put("d", "D");
        Map<String, Object> update = new LinkedHashMap<>();
        map.put("a", "A");
        map.put("b", "B");
        map.put("c", "C");
        assertTrue(Utils.mapChangesAfterUpdate(update, map, changes));
        Map<String, Object> expected = new LinkedHashMap<>();
        map.put("a", "A");
        map.put("b", "B");
        assertEquals(changes, expected);

        update.put("f", "F");
        assertFalse(Utils.mapChangesAfterUpdate(update, map, changes));
    }

    @Test
    void isMapEqualsTest() {
        Map<String, Object> mapA = null, mapB = null;

        assertTrue(Utils.isMapEquals(mapA, mapB));

        mapA = new LinkedHashMap<>();
        mapA.put("a", 1);
        mapA.put("b", 2);
        mapA.put("c", 3);

        assertFalse(Utils.isMapEquals(mapA, mapB));

        mapB = new LinkedHashMap<>();
        mapB.put("a", 1);
        mapB.put("b", 2);

        assertFalse(Utils.isMapEquals(mapA, mapB));

        mapB.put("c", "C");

        assertFalse(Utils.isMapEquals(mapA, mapB));

        mapB.put("c", 3);

        assertTrue(Utils.isMapEquals(mapA, mapB));
    }

    @AfterEach
    void tearDown() {
    }

    @AfterAll
    static void tearDownAll() {
    }
}
