/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.rdbcache.exceptions.ServerErrorException;
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
                add(new KvPair("*", object));
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

    public KvPair getAny() {
        return getAny(0);
    }

    public KvPair getAny(int index) {
        if (index > size()) {
            throw new ServerErrorException("getAny index out of range");
        } else if (index == size()) {
            add(new KvPair("*"));
        }
        return get(index);
    }

    public List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < size(); i++) {
            keys.add(get(i).getId());
        }
        return keys;
    }

    public String shortKey() {
        int size = size();
        if (size == 0) {
            return "";
        }
        String key = get(0).shortKey();
        if (size > 1) {
            key += "... ";
        }
        return key;
    }

}
