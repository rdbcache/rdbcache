package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.models.KvPair;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleRedisRepo implements RedisRepo {

    private Map<String, Object> data;

    public SimpleRedisRepo(Map<String, Object> map) {
        data = new LinkedHashMap<>(map);
    }

    @Override
    public boolean ifExist(Context context, KvPairs pairs, AnyKey anyKey) {
        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                foundAll = false;
                continue;
            } else {
                pair.setData(map);
            }
        }
        return foundAll;
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {
        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                foundAll = false;
                continue;
            } else {
                pair.setData(map);
            }
        }
        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            Map<String, Object> map = pair.getData();
            data.put(key, map);
        }
        return true;
    }

    @Override
    public boolean updateIfExist(Context context, KvPairs pairs, AnyKey anyKey) {
        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                foundAll = false;
            } else {
                Map<String, Object> update = pair.getData();
                for (Map.Entry<String, Object> entry: update.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                data.put(key, map);
            }
        }
        return foundAll;
    }

    @Override
    public boolean findAndSave(Context context, KvPairs pairs, AnyKey anyKey) {
        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                foundAll = false;
            } else {
                pair.setData(map);
            }
            data.put(key, pair.getData());
        }
        return foundAll;
    }

    @Override
    public void delete(Context context, KvPairs pairs, AnyKey anyKey, boolean dbOps) {
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            data.remove(key);
        }
    }
}
