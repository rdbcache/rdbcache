package com.rdbcache.helpers;

import com.rdbcache.models.KeyInfo;
import org.junit.Test;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UtilsTest {

    @Test
    public void toMapTestWithAssociateArrayJson() {
        String json = "{\"a\":1,\"b\":\"c\"}";
        Map<String, Object> map = Utils.toMap(json);
        Map<String, Object> newMap = new LinkedHashMap<>();
        newMap.put("a", 1);
        newMap.put("b", "c");
        assertEquals(map, newMap);
    }

    @Test
    public void toMapTestWithArrayJson() {
        String json = "[\"a\",\"b\",\"c\"]";
        Map<String, Object> map = Utils.toMap(json);
        Map<String, Object> newMap = new LinkedHashMap<>();
        newMap.put("0", "a");
        newMap.put("1", "b");
        newMap.put("2", "c");
        assertEquals(map, newMap);
    }

    @Test
    public void toMapTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        Map<String, Object> map = Utils.toMap(list);
        Map<String, Object> newMap = new LinkedHashMap<>();
        newMap.put("0", "a");
        newMap.put("1", "b");
        newMap.put("2", "c");
        assertEquals(map, newMap);
    }

    @Test
    public void toMapTestWithMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("0", "a");
        map.put("1", "b");
        map.put("2", "c");
        Map<String, Object> newMap = Utils.toMap(map);
        assertFalse(newMap == map);
        assertEquals(map, newMap);
    }

    @Test
    public void toMapTestWithPojo() {
        KeyInfo keyInfo = new KeyInfo("100", "table");
        Map<String, Object> map = Utils.toMap(keyInfo);
        String json = "{\"expire\":\"100\",\"table\":\"table\",\"clause\":\"\",\"params\":null,\"query_key\":\"\"}";
        Map<String, Object> newMap = Utils.toMap(json);
        assertEquals(map, newMap);
    }

    @Test
    public void toPojoTest() {
        KeyInfo keyInfo1 = new KeyInfo("100", "table");
        Map<String, Object> map = Utils.toMap(keyInfo1);
        KeyInfo keyInfo2 = Utils.toPojo(map, KeyInfo.class);
        assertEquals(keyInfo1, keyInfo2);
    }

    @Test
    public void toJsonTestWithAssociateArray() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "c");
        String json = Utils.toJson(map);
        assertEquals(json, "{\"a\":1,\"b\":\"c\"}");
    }

    @Test
    public void toJsonTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        String json = Utils.toJson(list);
        assertEquals(json, "{\"0\":\"a\",\"1\":\"b\",\"2\":\"c\"}");
    }

    @Test
    public void toPrettyJsonTestWithAssociateArray() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "c");
        String json = Utils.toPrettyJson(map).replaceAll("\\s", "");
        assertEquals(json, "{\"a\":1,\"b\":\"c\"}");
    }

    @Test
    public void toPrettyJsonTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        String json = Utils.toPrettyJson(list).replaceAll("\\s", "");
        assertEquals(json, "{\"0\":\"a\",\"1\":\"b\",\"2\":\"c\"}");
    }

    @Test
    public void generateIdTest() {
        String id = Utils.generateId();
        assertEquals(id.indexOf("-"), -1);
        assertEquals(id.indexOf(" "), -1);
        assertEquals(id.indexOf("\n"), -1);
    }

    @Test
    public void isValueEqualsTest() {
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
    public void mapChangesAfterUpdateTest() {

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
    public void isMapEqualsTest() {
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
}
