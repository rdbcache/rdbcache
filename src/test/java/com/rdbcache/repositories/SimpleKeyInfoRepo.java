package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleKeyInfoRepo implements KeyInfoRepo {

    private Map<String, Object> data;

    public SimpleKeyInfoRepo(Map<String, Object> map) {
        data = new LinkedHashMap<>(map);
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {
        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            String key = pair.getId();

        }
        return false;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {
        return false;
    }

    @Override
    public void delete(Context context, KvPairs pairs, boolean dbOps) {

    }
}
