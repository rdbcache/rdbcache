/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.models.KvPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KvPairs extends ArrayList<KvPair>{

    public KvPairs(String id, String value) {
        add(new KvPair(id, "data", value));
    }

    public KvPairs(String id) {
        add(new KvPair(id));
    }

    public KvPairs(List list) {
        for (Object object: list) {
            if (object instanceof  String) {
                String key = (String) object;
                add(new KvPair(key));
            } else if (object instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) object;
                if (map.containsKey("key")) {
                    String key = (String) map.get("key");
                    map.remove("key");
                    add(new KvPair(key, map));
                } else {
                    add(new KvPair("*", map));
                }
            }
        }
    }

    public KvPairs(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry: map.entrySet()) {
            KvPair pair = new KvPair(entry.getKey(), entry.getValue());
            add(pair);
        }
    }

    public KvPairs(KvPair pair) {
        add(pair);
    }

    public KvPairs() {
    }

    public List<KvPair> getPairs() {
        return this;
    }

    public void setPairs(List<KvPair> pairs) {
        clear();
        for (int i = 0; i < size(); i++) {
            add(pairs.get(i));
        }
    }

    public void setPair(KvPair pair) {
        clear();
        add(pair);
    }

    public KvPair getPair() {
        if (size() == 0) {
            return null;
        }
        return get(0);
    }

    public List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < size(); i++) {
            keys.add(get(i).getId());
        }
        return keys;
    }
}
