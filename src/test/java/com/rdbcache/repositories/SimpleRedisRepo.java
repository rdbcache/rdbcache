package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleRedisRepo implements RedisRepo {

    private Map<String, Object> data;

    public SimpleRedisRepo(Map<String, Object> map) {
        data = new LinkedHashMap<>(map);
    }

    @Override
    public boolean ifExist(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean updateIfExist(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean findAndSave(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public void delete(Context context, KvPairs pairs, AnyKey anyKey, boolean dbOps) {

    }
}
