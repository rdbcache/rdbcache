package doitincloud.commons.helpers;

import doitincloud.rdbcache.models.KvPair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import doitincloud.rdbcache.supports.KvPairs;
import org.junit.Test;

import static org.junit.Assert.*;

public class KvPairsTest {

    @Test
    public void listConstructor1() {

        List<String> list = new ArrayList<>();
        list.add("key0");
        list.add("key1");
        list.add("key2");
        KvPairs pairs = new KvPairs(list);

        assertEquals(3, pairs.size());
        assertEquals(list, pairs.getKeys());

    }

    @Test
    public void listConstructor2() {

        List<Map<String, Object>> list = new ArrayList<>();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k00", Utils.toMap("{\"kk00\": 0}"));
        map.put("k01", Utils.toMap("{\"kk01\": 1}"));
        map.put("k02", Utils.toMap("{\"kk02\": 2}"));
        list.add(map);
        map = new LinkedHashMap<>();
        map.put("k10", Utils.toMap("{\"kk10\": 0}"));
        map.put("k11", Utils.toMap("{\"kk11\": 1}"));
        map.put("k12", Utils.toMap("{\"kk12\": 2}"));
        list.add(map);
        map = new LinkedHashMap<>();
        map.put("k20", Utils.toMap("{\"kk20\": 0}"));
        map.put("k21", Utils.toMap("{\"kk21\": 1}"));
        map.put("k22", Utils.toMap("{\"kk22\": 2}"));
        list.add(map);

        KvPairs pairs = new KvPairs(list);
        assertEquals(3, pairs.size());
        for (int i = 0; i < 3; i++) {
            KvPair pair = pairs.get(i);
            map = pair.getData();
            assertTrue(pair.isNewUuid());
            assertEquals(3, map.size());
        }
    }

    @Test
    public void mapConstructor() {

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k0", Utils.toMap("{\"kk0\": 0}"));
        map.put("k1", Utils.toMap("{\"kk1\": 1}"));
        map.put("k2", Utils.toMap("{\"kk2\": 2}"));

        KvPairs pairs = new KvPairs(map);

        assertEquals(3, pairs.size());
        for (int i = 0; i < 3; i++) {
            KvPair pair = pairs.get(i);
            assertEquals("k"+i, pair.getId());
            map = pair.getData();
            assertEquals(i, map.get("kk"+i));
        }
    }

    @Test
    public void setPair() {

        KvPairs pairs;
        KvPair pair;

        pairs = new KvPairs();
        pair = new KvPair();
        pairs.setPair(pair);

        assertEquals(1, pairs.size());
        assertTrue(pair == pairs.get(0));

        pairs = new KvPairs(new KvPair());
        pair = new KvPair("key", "data", "value");
        pairs.setPair(pair);

        assertEquals(1, pairs.size());
        assertTrue(pair == pairs.get(0));
    }

    @Test
    public void getPair() {

        KvPairs pairs;
        KvPair pair;

        pairs = new KvPairs();
        pair = pairs.getPair();

        assertNull(pair);

        pair = new KvPair("key", "data", "value");
        pairs = new KvPairs(pair);

        assertTrue(pair == pairs.get(0));

        KvPair pair2 = pairs.getPair();

        assertNotNull(pair2);
        assertTrue(pair == pair2);
    }

    @Test
    public void getAny() {

        KvPairs pairs;
        KvPair pair;

        pairs = new KvPairs();
        pair = pairs.getAny();
        assertNotNull(pair);

        pair = new KvPair("key", "data", "value");
        pairs = new KvPairs(pair);

        KvPair pair2 = pairs.getAny();

        assertNotNull(pair2);
        assertTrue(pair == pair2);
        assertTrue(pair == pairs.get(0));

        for (int i = 0; i < 10; i++) {
            pair = pairs.getAny(i);
            assertNotNull(pair);
            assertEquals(i+1, pairs.size());
        }
    }

    @Test
    public void getKeys() {

        KvPairs pairs;
        KvPair pair;

        pairs = new KvPairs();
        for (int i = 0; i < 10; i++) {
            pairs.add(new KvPair("key" + i, "data", "value" + i));
        }
        List<String> keys = pairs.getKeys();
        for (int i = 0; i < 10; i++) {
            assertEquals("key" + i, keys.get(i));
        }
    }
}