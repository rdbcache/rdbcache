package doitincloud.rdbcache.models;

import doitincloud.commons.helpers.Utils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class KvPairTest {

    @Test
    public void convertJsonPojoMap() {
        Map<String, Object> map = Utils.toMap("{\n" +
                "    \"expire\" : \"30\",\n" +
                "    \"table\" : \"user_table\",\n" +
                "    \"clause\" : \"id = ?\",\n" +
                "    \"params\" : [ 12466 ],\n" +
                "    \"query_key\" : \"28f0a2d90b3c9d340e853b838d27845c\"\n" +
                "  }");

        KvPair pair1 = new KvPair("*", "keyInfo", map);

        Map<String, Object> pairMap1 = Utils.toMap(pair1);

        //System.out.println(Utils.toJsonMap(pairMap1));

        KvPair pair2 = Utils.toPojo(pairMap1, KvPair.class);

        assertEquals(pair1, pair2);

        Map<String, Object> pairMap2 = Utils.toMap(pair2);

        assertEquals(pairMap1, pairMap2);
    }
}