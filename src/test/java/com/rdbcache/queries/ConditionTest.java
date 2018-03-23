package com.rdbcache.queries;

import com.rdbcache.helpers.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

public class ConditionTest {

    @Test
    public void convertJsonPojoMap() {

        Condition condition1 = new Condition("=", new String[]{"1", "2", "3"});
        Map<String, Object> map1 = Utils.toMap(condition1);
        Condition condition2 = Utils.toPojo(map1, Condition.class);
        Map<String, Object> map2 = Utils.toMap("{\"=\":[\"1\",\"2\",\"3\"]}");

        assertEquals(map1, map2);
        assertEquals(condition1, condition2);
    }

}