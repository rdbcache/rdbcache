package com.rdbcache.models;

import com.rdbcache.helpers.Utils;
import com.rdbcache.queries.Condition;
import com.rdbcache.queries.QueryInfo;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class KeyInfoTest {

    @Test
    public void objectToMap1() {

        KeyInfo keyInfo1 = new KeyInfo();
        Map<String, Object> map1 = Utils.toMap(keyInfo1);
        KeyInfo keyInfo2 = Utils.toPojo(map1, KeyInfo.class);
        Map<String, Object> map2 = Utils.toMap(keyInfo2);

        assertEquals(keyInfo1, keyInfo2);
        assertEquals(map1, map2);
        //System.out.println(Utils.toJsonMap(map1));
    }

    @Test
    public void objectToMap2() {

        KeyInfo keyInfo1 = new KeyInfo();
        keyInfo1.setExpire("100");
        keyInfo1.setTable("table");
        Map<String, Object> map1 = Utils.toMap(keyInfo1);
        KeyInfo keyInfo2 = Utils.toPojo(map1, KeyInfo.class);
        Map<String, Object> map2 = Utils.toMap(keyInfo2);

        assertEquals(keyInfo1, keyInfo2);
        assertEquals(map1, map2);
        //System.out.println(Utils.toJsonMap(map1));
    }

    @Test
    public void objectToMap3() {

        Map<String, Object> map1 = Utils.toMap("{\"expire\":\"30\",\"table\":\"user_table\",\"clause\":"+
                "\"id = ?\",\"params\":[12466],\"query_key\":\"1e44ca12f7ec6d2ee835c94bdc2c01dc\",\"is_new\":false,"+
                "\"expire_old\":\"180\"}");
        KeyInfo keyInfo1 = Utils.toPojo(map1, KeyInfo.class);
        Map<String, Object> map2 = Utils.toMap(keyInfo1);
        assertNotNull(map2.get("created_at"));
        map2.remove("created_at");
        KeyInfo keyInfo2 = Utils.toPojo(map2, KeyInfo.class);

        //System.out.println(Utils.toJsonMap(map2));

        assertEquals(keyInfo1, keyInfo2);
        assertEquals(map1, map2);
    }

    @Test
    public void objectToMap4() {

        Map<String, Object> map1 = Utils.toMap("{\"expire\":\"30\",\"table\":\"user_table\",\"clause\":"+
                "\"id = ?\",\"params\":[12466],\"query\":{\"table\":\"user_table\",\"conditions\":{\"id\":"+
                "{\"=\":[\"12466\"]}}},\"query_key\":\"28f0a2d90b3c9d340e853b838d27845c\",\"is_new\":"+
                "false,\"expire_old\":\"180\",\"created_at\":1522367412704}");
        KeyInfo keyInfo1 = Utils.toPojo(map1, KeyInfo.class);

        QueryInfo queryInfo = new QueryInfo("user_table");
        String params[] = {"12466"};
        Condition condition = new Condition("=", params);
        Map<String, Condition> conditions = new LinkedHashMap<>();
        conditions.put("id", condition);
        queryInfo.setConditions(conditions);
        keyInfo1.setQuery(queryInfo);

        Map<String, Object> map2 = Utils.toMap(keyInfo1);
        KeyInfo keyInfo2 = Utils.toPojo(map2, KeyInfo.class);

        //System.out.println(Utils.toJsonMap(map2));

        assertEquals(keyInfo1, keyInfo2);
        assertEquals(map1, map2);
    }

    @Test public void cloneTest() {

        Map<String, Object> map1 = Utils.toMap("{\n" +
                "    \"expire\" : \"30\",\n" +
                "    \"table\" : \"user_table\",\n" +
                "    \"clause\" : \"id = ?\",\n" +
                "    \"params\" : [ 12466 ],\n" +
                "    \"query_key\" : \"28f0a2d90b3c9d340e853b838d27845c\"\n" +
                "  }");
        KeyInfo keyInfo1 = Utils.toPojo(map1, KeyInfo.class);

        KeyInfo keyInfo2 = keyInfo1.clone();

        assertFalse(keyInfo1.getParams() == keyInfo2.getParams());
        keyInfo2.setExpire("60");
        assertNotEquals(keyInfo1.getExpire(), keyInfo2.getExpire());
        keyInfo2.setTable("types_table");
        assertNotEquals(keyInfo1.getTable(), keyInfo2.getTable());
        keyInfo2.setClause("id != ?");
        assertNotEquals(keyInfo1.getClause(), keyInfo2.getClause());
        assertNotEquals(keyInfo1, keyInfo2);
    }
}