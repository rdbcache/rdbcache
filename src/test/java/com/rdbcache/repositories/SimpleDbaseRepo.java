package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleDbaseRepo implements DbaseRepo {

    private Map<String, Object> data;

    public SimpleDbaseRepo(Map<String, Object> map) {
        data = new LinkedHashMap<>(map);
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
    public boolean insert(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean update(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public boolean delete(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }
}
