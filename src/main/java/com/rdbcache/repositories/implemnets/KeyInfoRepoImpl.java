/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.rdbcache.repositories.implemnets;

import com.rdbcache.helpers.Config;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.*;
import com.rdbcache.repositories.KeyInfoRepo;
import com.rdbcache.services.AsyncOps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class KeyInfoRepoImpl implements KeyInfoRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyInfoRepoImpl.class);

    private RedisTemplate<String, KeyInfo> keyInfoTemplate;

    private HashOperations<String, String, KeyInfo> keyInfoOps;

    @Autowired
    public KeyInfoRepoImpl(RedisTemplate keyInfoTemplate) {
        this.keyInfoTemplate = keyInfoTemplate;
    }

    @PostConstruct
    public void init() {
        keyInfoOps = keyInfoTemplate.opsForHash();
    }

    @Override
    public KeyInfo findOne(Context context) {

        KvPair pair = context.getPair();
        if (pair == null) {
            return null;
        }
        String key = pair.getId();

        LOGGER.trace("findOne: " + key);

        KeyInfo keyInfo = AppCtx.getLocalCache().getKeyInfo(key);
        if (keyInfo != null) {
            LOGGER.debug("found from local cache");
            return keyInfo;
        }

        StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.get");
        keyInfo = keyInfoOps.get(Config.getHkeyPrefix() + "::keyinfo", key);
        if (stopWatch != null) stopWatch.stopNow();

        if (keyInfo != null) {
            LOGGER.debug("found from redis");
            AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            return keyInfo;
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "info"));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null || dbPair.getData() == null) {
            LOGGER.debug("keyinfo not found from database for " + key);
            return null;
        }
        LOGGER.debug("found key info from database for " + key);

        keyInfo = new KeyInfo(dbPair.getData());
        AppCtx.getLocalCache().putKeyInfo(key, keyInfo);

        final KeyInfo finalKeyInfo = keyInfo;
        AsyncOps.getExecutor().submit(() -> {
            Thread.yield();
            StopWatch stopWatch2 = context.startStopWatch("redis", "keyInfoOps.put");
            keyInfoOps.put(Config.getHkeyPrefix() + "::keyinfo", key, finalKeyInfo);
            if (stopWatch2 != null) stopWatch2.stopNow();
        });

        return keyInfo;
    }

    @Override
    public List<KeyInfo> findAll(Context context) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("findAll: " + pairs.size());

        if (pairs.size() == 0) {
            return null;
        }
        List<String> keys = new ArrayList<String>();
        for (KvPair pair: pairs) {
            keys.add(pair.getId());
        }
        List<KeyInfo> keyInfos = new ArrayList<KeyInfo>();
        if (AppCtx.getLocalCache().getKeyInfos(keys, keyInfos)) {
            return keyInfos;
        }

        int i = 0;
        List<String> redisKeys = new ArrayList<String>();
        for (String key: keys) {
            if (keyInfos.get(i) == null) {
                redisKeys.add(key);
            }
            i++;
        }

        StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.multiGet");
        List<KeyInfo> redisKeyInfos = keyInfoOps.multiGet(Config.getHkeyPrefix() + "::keyinfo", redisKeys);
        if (stopWatch != null) stopWatch.stopNow();

        i = 0;
        boolean findAll = true;
        for (String key: redisKeys) {
            if (redisKeyInfos.get(i) == null) {
                findAll = false;
                continue;
            }
            AppCtx.getLocalCache().put(key, redisKeyInfos.get(i));
            int index = keys.indexOf(key);
            keyInfos.set(index, redisKeyInfos.get(index));
            i++;
        }

        if (findAll) {
            return keyInfos;
        }

        i = 0;
        List<KvIdType> idTypes = new ArrayList<KvIdType>();
        for (String key: keys) {
            if (keyInfos.get(i) == null) {
                idTypes.add(new KvIdType(key, "info"));
            }
            i++;
        }

        stopWatch = context.startStopWatch("redis", "kvPairRepo.findAll");
        Iterable<KvPair> dbKeyInfos = AppCtx.getKvPairRepo().findAll(idTypes);
        if (stopWatch != null) stopWatch.stopNow();

        Map<String, KeyInfo> redisKeyInfoMap = new LinkedHashMap<String, KeyInfo>();
        Iterator<KvPair> iterator = dbKeyInfos.iterator();
        while (iterator.hasNext()) {
            KvPair dbPair = iterator.next();
            String key = dbPair.getId();
            int index= keys.indexOf(key);
            KeyInfo keyInfo = new KeyInfo(dbPair.getData());
            AppCtx.getLocalCache().put(key, keyInfo);
            redisKeyInfoMap.put(key, keyInfo);
            keyInfos.set(index, keyInfo);
        }

        AsyncOps.getExecutor().submit(() -> {

            Thread.yield();

            StopWatch stopWatch2 = context.startStopWatch("dbase", "finalQuery.save");
            keyInfoOps.putAll(Config.getHkeyPrefix() + "::keyinfo", redisKeyInfoMap);
            if (stopWatch2 != null) stopWatch2.stopNow();

        });

        return keyInfos;
    }

    @Override
    public void saveOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();

        LOGGER.trace("saveOne: " + key + " table: " + keyInfo.getTable());

        KeyInfo cachedKeyInfo = AppCtx.getLocalCache().getKeyInfo(key);
        if (cachedKeyInfo != null) {
            if (keyInfo.getTable() == null && cachedKeyInfo.getTable() != null) {
                keyInfo.setTable(cachedKeyInfo.getTable());
            }
            if (keyInfo.getClause() == null && cachedKeyInfo.getClause() != null) {
                keyInfo.setClause(cachedKeyInfo.getClause());
            }
            if (keyInfo.getParams() == null && cachedKeyInfo.getParams() != null) {
                keyInfo.setParams(cachedKeyInfo.getParams());
            }
            if (!keyInfo.hasStdPKClause(context) && cachedKeyInfo.hasStdPKClause(context)) {
                keyInfo.setClause(cachedKeyInfo.getClause());
                keyInfo.setParams(cachedKeyInfo.getParams());
            }
            if (cachedKeyInfo.equals(keyInfo)) {
                keyInfo.setIsNew(false);
                return;
            }
        }
        AppCtx.getLocalCache().putKeyInfo(key, keyInfo);

        final Query finalQuery = keyInfo.getQuery();
        if (finalQuery != null) {
            keyInfo.setQuery(null);
            AsyncOps.getExecutor().submit(() -> {

                Thread.yield();

                StopWatch stopWatch = context.startStopWatch("dbase", "finalQuery.save");
                finalQuery.save(context);
                if (stopWatch != null) stopWatch.stopNow();
            });
        }

        final KeyInfo finalKeyInfo = keyInfo;
        AsyncOps.getExecutor().submit(() -> {

            Thread.yield();

            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.put");
            keyInfoOps.put(Config.getHkeyPrefix() + "::keyinfo", key, finalKeyInfo);
            if (stopWatch != null) stopWatch.stopNow();

            KvPair dbPair = new KvPair(key, "info", finalKeyInfo.toMap());

            stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(dbPair);
            if (stopWatch != null) stopWatch.stopNow();
        });
    }

    @Override
    public void saveAll(Context context, List<KeyInfo> keyInfos) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("saveAll: " + pairs.size() + " keyInfos: " + keyInfos.size());

        if (pairs.size() == 0 || keyInfos.size() == 0) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (KvPair pair: pairs) {
            keys.add(pair.getId());
        }
        List<KeyInfo> cachedKeyInfos = new ArrayList<KeyInfo>();
        AppCtx.getLocalCache().getKeyInfos(keys, cachedKeyInfos);

        List<String> todoKeys = new ArrayList<String>();
        List<KeyInfo> todoKeyInfos = new ArrayList<KeyInfo>();

        int i = 0;
        for (String key: keys) {

            KeyInfo cachedKeyInfo = cachedKeyInfos.get(i);
            KeyInfo keyInfo = keyInfos.get(i++);

            if (cachedKeyInfo != null) {
                if (keyInfo.getTable() == null && cachedKeyInfo.getTable() != null) {
                    keyInfo.setIsNew(false);
                    continue;
                }
                if (keyInfo.getClause() == null && cachedKeyInfo.getClause() != null) {
                    keyInfo.setIsNew(false);
                    continue;
                }
                if (keyInfo.getParams() == null && cachedKeyInfo.getParams() != null) {
                    keyInfo.setIsNew(false);
                    continue;
                }
                if (cachedKeyInfo.equals(keyInfo)) {
                    keyInfo.setIsNew(false);
                    continue;
                }
                if (cachedKeyInfo.hasStdPKClause(context)) {
                    if (cachedKeyInfo.getClause().equals(keyInfo.getClause()) &&
                            keyInfo.getParams().equals(cachedKeyInfo.getParams())) {
                        keyInfo.setIsNew(false);
                        return;
                    }
                }
            }
            AppCtx.getLocalCache().put(key, keyInfo);
            todoKeys.add(key);
            todoKeyInfos.add(keyInfo);
        }
        if (todoKeys.size() == 0) {
            return;
        }
        final List<String> finalKeys = todoKeys;
        final List<KeyInfo> finalKeyInfos = todoKeyInfos;

        AsyncOps.getExecutor().submit(() -> {

            Thread.yield();

            int index = 0;
            for (String key: finalKeys) {
                KeyInfo keyInfoPer = finalKeyInfos.get(index++);

                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.putAll");
                keyInfoOps.put(Config.getHkeyPrefix() + "::keyinfo", key, keyInfoPer);
                if (stopWatch != null) stopWatch.stopNow();
            }

            List<KvPair> dbPairs = new ArrayList<KvPair>();
            index = 0;
            for (String key: finalKeys) {
                dbPairs.add(new KvPair(key, "info", finalKeyInfos.get(index++).toMap()));
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(dbPairs);
            if (stopWatch != null) stopWatch.stopNow();
        });

    }

    @Override
    public void deleteOne(Context context) {
        KvPair pair = context.getPair();
        String key = pair.getId();

        AppCtx.getLocalCache().removeKeyInfo(key);

        StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
        keyInfoOps.delete(Config.getHkeyPrefix() + "::keyinfo", key);
        if (stopWatch != null) stopWatch.stopNow();
    }

    @Override
    public void deleteAll(Context context) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("deleteAll: " + pairs.size());

        if (pairs.size() == 0) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (KvPair pair: pairs) {
            String key = pair.getId();
            keys.add(key);
            AppCtx.getLocalCache().removeKeyInfo(key);
        }

        StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
        keyInfoOps.delete(Config.getHkeyPrefix() + "::keyinfo", keys);
        if (stopWatch != null) stopWatch.stopNow();

    }
}