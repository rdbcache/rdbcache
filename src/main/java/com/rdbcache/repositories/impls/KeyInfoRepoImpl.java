/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.*;
import com.rdbcache.repositories.KeyInfoRepo;
import com.rdbcache.services.AsyncOps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
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

    private boolean enableLocalCache = true;

    private boolean enableRedisCache = true;

    private String hkeyPrefix = Cfg.getHkeyPrefix();

    @Autowired
    private RedisTemplate<String, KeyInfo> keyInfoTemplate;

    private HashOperations<String, String, KeyInfo> keyInfoOps;

    @PostConstruct
    public void init() {
        keyInfoOps = keyInfoTemplate.opsForHash();
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        hkeyPrefix = Cfg.getHkeyPrefix();
        if (Cfg.getDataMaxCacheTLL() <= 0l) {
            enableLocalCache = false;
        }
    }

    public boolean isEnableLocalCache() {
        return enableLocalCache;
    }

    public void setEnableLocalCache(boolean enableLocalCache) {
        this.enableLocalCache = enableLocalCache;
    }

    public boolean isEnableRedisCache() {
        return enableRedisCache;
    }

    public void setEnableRedisCache(boolean enableRedisCache) {
        this.enableRedisCache = enableRedisCache;
    }

    public String getHkeyPrefix() {
        return hkeyPrefix;
    }

    public void setHkeyPrefix(String hkeyPrefix) {
        this.hkeyPrefix = hkeyPrefix;
    }

    @Override
    public KeyInfo findOne(Context context) {

        KvPair pair = context.getPair();
        if (pair == null) {
            return null;
        }
        String key = pair.getId();

        LOGGER.trace("findOne: " + key);

        KeyInfo keyInfo = null;

        if (enableLocalCache) {
            keyInfo  = AppCtx.getLocalCache().getKeyInfo(key);
            if (keyInfo != null) {
                LOGGER.debug("found from local cache");
                return keyInfo;
            }
        }

        StopWatch stopWatch = null;

        if (enableRedisCache) {

            stopWatch = context.startStopWatch("redis", "keyInfoOps.get");
            keyInfo = keyInfoOps.get(hkeyPrefix + "::keyinfo", key);
            if (stopWatch != null) stopWatch.stopNow();

            if (keyInfo != null) {
                LOGGER.debug("found from redis");
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                return keyInfo;
            }
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "info"));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null || dbPair.getData() == null) {
            LOGGER.debug("keyinfo not found from database for " + key);
            return null;
        }
        LOGGER.debug("found key info from database for " + key);

        final KeyInfo finalKeyInfo = new KeyInfo(dbPair.getData());

        if (enableLocalCache) {
            AppCtx.getLocalCache().putKeyInfo(key, finalKeyInfo);
        }

        if (enableRedisCache) {
            AsyncOps.getExecutor().submit(() -> {
                Thread.yield();
                StopWatch stopWatch2 = context.startStopWatch("redis", "keyInfoOps.put");
                keyInfoOps.put(hkeyPrefix + "::keyinfo", key, finalKeyInfo);
                if (stopWatch2 != null) stopWatch2.stopNow();
            });
        }

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
        List<KeyInfo> keyInfos = new ArrayList<KeyInfo>();
        List<String> redisKeys = new ArrayList<String>();
        List<KvIdType> idTypes = new ArrayList<KvIdType>();

        boolean foundAll = true;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            keys.add(key);

            KeyInfo keyInfo = null;

            if (enableLocalCache) {
                keyInfo = AppCtx.getLocalCache().getKeyInfo(key);
                keyInfos.add(keyInfo);
            }

            if (keyInfo == null) {
                foundAll = false;
                if (enableRedisCache) {
                    redisKeys.add(key);
                } else {
                    idTypes.add(new KvIdType(key, "info"));
                }
            }
        }

        if (foundAll) {
            return keyInfos;
        }

        StopWatch stopWatch = null;

        if (redisKeys.size() > 0) {

            List<KeyInfo> redisKeyInfos = null;
            stopWatch = context.startStopWatch("redis", "keyInfoOps.multiGet");
            redisKeyInfos = keyInfoOps.multiGet(hkeyPrefix + "::keyinfo", redisKeys);
            if (stopWatch != null) stopWatch.stopNow();

            foundAll = true;
            int index = 0;
            for (int i = 0; i < redisKeys.size(); i++) {

                String key = redisKeys.get(i);
                KeyInfo keyInfo = redisKeyInfos.get(i);

                for (; index < keyInfos.size(); index++) {
                    if (keyInfos.get(index) == null) {
                        break;
                    }
                }

                if (keyInfo == null) {
                    foundAll = false;
                    idTypes.add(new KvIdType(key, "info"));
                } else {
                    if (enableLocalCache) {
                        AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                    }
                    keyInfos.set(index, keyInfo);
                }
            }

            if (foundAll) {
                return keyInfos;
            }
        }

        stopWatch = context.startStopWatch("redis", "kvPairRepo.findAll");
        Iterable<KvPair> dbKeyInfos = AppCtx.getKvPairRepo().findAll(idTypes);
        if (stopWatch != null) stopWatch.stopNow();

        final Map<String, KeyInfo> redisKeyInfoMap = new LinkedHashMap<String, KeyInfo>();

        Iterator<KvPair> iterator = dbKeyInfos.iterator();
        while (iterator.hasNext()) {

            KvPair dbPair = iterator.next();
            if (dbPair == null) {
                continue;
            }
            String key = dbPair.getId();
            KeyInfo keyInfo = new KeyInfo(dbPair.getData());

            int index = keys.indexOf(key);
            keys.set(index, null);
            keyInfos.set(index, keyInfo);

            if (enableLocalCache) {
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            }
            if (enableRedisCache) {
                redisKeyInfoMap.put(key, keyInfo);
            }
        }

        if (redisKeyInfoMap.size() > 0) {
            AsyncOps.getExecutor().submit(() -> {

                Thread.yield();

                StopWatch stopWatch2 = context.startStopWatch("dbase", "finalQuery.save");
                keyInfoOps.putAll(hkeyPrefix + "::keyinfo", redisKeyInfoMap);
                if (stopWatch2 != null) stopWatch2.stopNow();

            });
        }
        return keyInfos;
    }

    @Override
    public void saveOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();

        LOGGER.trace("saveOne: " + key + " table: " + keyInfo.getTable());

        if (enableLocalCache) {

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
        }

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

            StopWatch stopWatch = null;

            if (enableRedisCache) {

                stopWatch = context.startStopWatch("redis", "keyInfoOps.put");
                keyInfoOps.put(hkeyPrefix + "::keyinfo", key, finalKeyInfo);
                if (stopWatch != null) stopWatch.stopNow();
            }

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

        List<String> todoKeys = new ArrayList<String>();
        List<KeyInfo> todoKeyInfos = new ArrayList<KeyInfo>();

        int i = 0;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            KeyInfo keyInfo = keyInfos.get(i++);

            if (enableLocalCache) {

                KeyInfo cachedKeyInfo = AppCtx.getLocalCache().getKeyInfo(key);

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
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            }
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

            if (enableRedisCache) {
                int index = 0;
                for (String key : finalKeys) {
                    KeyInfo keyInfoPer = finalKeyInfos.get(index++);

                    StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.putAll");
                    keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfoPer);
                    if (stopWatch != null) stopWatch.stopNow();
                }
            }

            List<KvPair> dbPairs = new ArrayList<KvPair>();
            int index = 0;
            for (String key: finalKeys) {
                dbPairs.add(new KvPair(key, "info", finalKeyInfos.get(index++).toMap()));
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(dbPairs);
            if (stopWatch != null) stopWatch.stopNow();
        });

    }

    @Override
    public void deleteOne(Context context, boolean dbOps) {
        KvPair pair = context.getPair();
        String key = pair.getId();

        if (enableLocalCache) {

            AppCtx.getLocalCache().removeKeyInfo(key);
        }

        if (enableRedisCache) {
            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
            keyInfoOps.delete(hkeyPrefix + "::keyinfo", key);
            if (stopWatch != null) stopWatch.stopNow();
        }

        if (dbOps) {
            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.delete");
            AppCtx.getKvPairRepo().delete(new KvIdType(key, "info"));
            if (stopWatch != null) stopWatch.stopNow();
        }
    }

    @Override
    public void deleteAll(Context context, boolean dbOps) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("deleteAll: " + pairs.size());

        if (pairs.size() == 0) {
            return;
        }

        List<String> keys = new ArrayList<String>();
        for (KvPair pair: pairs) {
            String key = pair.getId();
            keys.add(key);
            if (enableLocalCache) {
                AppCtx.getLocalCache().removeKeyInfo(key);
            }
        }

        if (enableRedisCache) {
            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
            keyInfoOps.delete(hkeyPrefix + "::keyinfo", keys);
            if (stopWatch != null) stopWatch.stopNow();
        }

        if (dbOps) {
            for (KvPair pair: pairs) {
                String key = pair.getId();
                StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.delete");
                AppCtx.getKvPairRepo().delete(new KvIdType(key, "info"));
                if (stopWatch != null) stopWatch.stopNow();
            }
        }

    }
}