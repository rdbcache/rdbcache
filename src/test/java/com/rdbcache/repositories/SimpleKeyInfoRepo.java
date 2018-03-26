package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleKeyInfoRepo implements KeyInfoRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKeyInfoRepo.class);

    private Map<String, Object> data;

    public SimpleKeyInfoRepo(Map<String, Object> map) {
        data = new LinkedHashMap<>(map);
    }

    public SimpleKeyInfoRepo() {
        data = new LinkedHashMap<>();
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("find pairs(" + pairs.size() + ") anyKey(" + anyKey.size() + ")");

        boolean foundAll = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            String key = pair.getId();
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                foundAll = false;
                LOGGER.trace("find: Not Found " + key);
                continue;
            } else {
                keyInfo = Utils.toPojo(map,  KeyInfo.class);
                anyKey.set(i, keyInfo);
                LOGGER.trace("find: Found " + key);
            }
        }
        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        Assert.isTrue(anyKey.size() == pairs.size(), anyKey.size() + " != " +
                pairs.size() + ", only supports that pairs and anyKey have the same size");

        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            String key = pair.getId();
            Map<String, Object> map = Utils.toMap(keyInfo);
            data.put(key, map);
            LOGGER.trace("save: " + key);
        }
        return true;
    }

    @Override
    public void delete(Context context, KvPairs pairs, boolean dbOps) {

        LOGGER.trace("delete(" + pairs.size() + "): " + pairs.printKey());

        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            String key = pair.getId();
            data.remove(key);
            LOGGER.trace("delete: " + key);
        }
    }
}
