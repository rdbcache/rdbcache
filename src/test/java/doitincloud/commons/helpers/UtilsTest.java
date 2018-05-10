package doitincloud.commons.helpers;

import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.supports.DbUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.*;

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
        KeyInfo keyInfo = new KeyInfo();
        keyInfo.setExpire("100");
        keyInfo.setTable("table");
        keyInfo.cleanup();
        Map<String, Object> map = Utils.toMap(keyInfo);
        //System.out.println(Utils.toJsonMap(map));
        assertNotNull(map.get("created_at"));
        map.remove("created_at");
        String json = "{\"expire\":\"100\",\"table\":\"table\",\"clause\":\"\",\"query_key\":\"\",\"is_new\":false}";
        Map<String, Object> newMap = Utils.toMap(json);
        assertEquals(map, newMap);
    }

    @Test
    public void toPojoTest() {
        KeyInfo keyInfo1 = new KeyInfo();
        keyInfo1.setExpire("100");
        keyInfo1.setTable("table");
        Map<String, Object> map = Utils.toMap(keyInfo1);
        KeyInfo keyInfo2 = Utils.toPojo(map, KeyInfo.class);
        assertEquals(keyInfo1, keyInfo2);
    }

    @Test
    public void toJsonTestWithAssociateArray() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "c");
        String json = Utils.toJsonMap(map);
        assertEquals(json, "{\"a\":1,\"b\":\"c\"}");
    }

    @Test
    public void toJsonTestWithArray() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        String json = Utils.toJsonMap(list);
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
        assertEquals("[\"a\",\"b\",\"c\"]", json);
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
        assertTrue(DbUtils.isValueEquals(null, null));
        assertFalse(DbUtils.isValueEquals(null, ""));
        assertFalse(DbUtils.isValueEquals("", null));

        String a = "string a";
        String b = "string b";
        String c = "string a";

        assertTrue(DbUtils.isValueEquals(a, a));
        assertTrue(DbUtils.isValueEquals(a, c));
        assertFalse(DbUtils.isValueEquals(a, b));

        a = "string a ";
        b = " string a";
        assertTrue(DbUtils.isValueEquals(a, b));

        Boolean boolTrue = true, boolFalse = false;
        assertTrue(DbUtils.isValueEquals(boolTrue, 1));
        assertTrue(DbUtils.isValueEquals(boolTrue, "true"));
        assertTrue(DbUtils.isValueEquals(boolTrue, "TRUE"));
        assertTrue(DbUtils.isValueEquals(boolFalse, 0));
        assertTrue(DbUtils.isValueEquals(boolFalse, "false"));
        assertTrue(DbUtils.isValueEquals(boolFalse, "FALSE"));

        Double doubleA = 3.1415926;
        Float floatA = 3.141592F;
        BigDecimal bdA = new BigDecimal("3.1415926");
        assertTrue(DbUtils.isValueEquals(doubleA, "3.1415926"));
        assertTrue(DbUtils.isValueEquals(floatA, "3.141592"));
        assertTrue(DbUtils.isValueEquals(bdA, "3.1415926"));

        Long longA = 31415926L;
        Integer integerA = 3141592;
        assertTrue(DbUtils.isValueEquals(longA, "31415926"));
        assertTrue(DbUtils.isValueEquals(integerA, "3141592"));
    }

    @Test
    public void mapChangesAfterUpdateTest() {

        Map<String, Object> changes = new LinkedHashMap<>();
        assertFalse(DbUtils.mapChangesAfterUpdate(null, null, changes, new ArrayList<>()));

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", "C");
        map.put("d", "D");
        Map<String, Object> update = new LinkedHashMap<>();
        map.put("a", "A");
        map.put("b", "B");
        map.put("c", "C");
        assertTrue(DbUtils.mapChangesAfterUpdate(update, map, changes, new ArrayList<>()));
        Map<String, Object> expected = new LinkedHashMap<>();
        map.put("a", "A");
        map.put("b", "B");
        assertEquals(changes, expected);

        update.put("f", "F");
        assertFalse(DbUtils.mapChangesAfterUpdate(update, map, changes, new ArrayList<>()));
    }

    @Test
    public void isMapEqualsTest() {
        Map<String, Object> mapA = null, mapB = null;

        assertTrue(DbUtils.isMapEquals(mapA, mapB));

        mapA = new LinkedHashMap<>();
        mapA.put("a", 1);
        mapA.put("b", 2);
        mapA.put("c", 3);

        assertFalse(DbUtils.isMapEquals(mapA, mapB));

        mapB = new LinkedHashMap<>();
        mapB.put("a", 1);
        mapB.put("b", 2);

        assertFalse(DbUtils.isMapEquals(mapA, mapB));

        mapB.put("c", "C");

        assertFalse(DbUtils.isMapEquals(mapA, mapB));

        mapB.put("c", 3);

        assertTrue(DbUtils.isMapEquals(mapA, mapB));
    }
}
