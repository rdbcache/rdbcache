package com.rdbcache.queries;

import com.rdbcache.helpers.Utils;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class QueryInfoTest {

    @Test
    public void convertJsonPojoMap() {

        QueryInfo queryInfo1 = new QueryInfo("user_table");
        queryInfo1.setLimit(2);
        Condition condition = new Condition("=", new String[]{"1", "2", "3"});
        Map<String, Condition> conditions = new LinkedHashMap<>();
        conditions.put("id", condition);
        queryInfo1.setConditions(conditions);
        Map<String, Object> map1 = Utils.toMap(queryInfo1);

        String json = "{\"table\":\"user_table\",\"conditions\":{\"id\":{\"=\":[\"1\",\"2\",\"3\"]}},\"limit\":2}";
        Map<String, Object> map2 = Utils.toMap(json);
        QueryInfo queryInfo2 = Utils.toPojo(map2, QueryInfo.class);

        assertEquals(map1, map2);
        assertEquals(queryInfo1, queryInfo2);
        assertEquals(queryInfo1.getKey(), queryInfo2.getKey());
    }
}