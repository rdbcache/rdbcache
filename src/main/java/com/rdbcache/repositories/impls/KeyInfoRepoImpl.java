/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.helpers.*;
import com.rdbcache.models.*;
import com.rdbcache.queries.QueryInfo;
import com.rdbcache.repositories.KeyInfoRepo;
import com.rdbcache.queries.Query;
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

    private String hkeyPrefix = PropCfg.getHkeyPrefix();

    @Autowired
    private RedisTemplate<String, KeyInfo> keyInfoTemplate;

    private HashOperations<String, String, KeyInfo> keyInfoOps;

    @PostConstruct
    public void init() {
        keyInfoOps = keyInfoTemplate.opsForHash();
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        hkeyPrefix = PropCfg.getHkeyPrefix();
        if (PropCfg.getKeyMinCacheTTL() <= 0l && PropCfg.getDataMaxCacheTLL() <= 0l) {
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
    public boolean find(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        if (pair == null) {
            return false;
        }
        String key = pair.getId();

        LOGGER.trace("find: " + key);

        if (enableLocalCache) {
            KeyInfo keyInfoCache  = AppCtx.getLocalCache().getKeyInfo(key);
            if (keyInfoCache != null) {
                LOGGER.debug("found from local cache");
                keyInfo.copy(keyInfoCache);
                return true;
            }
        }

        StopWatch stopWatch = null;

        if (enableRedisCache) {

            stopWatch = context.startStopWatch("redis", "keyInfoOps.get");
            KeyInfo keyInfoRedis = keyInfoOps.get(hkeyPrefix + "::keyinfo", key);
            if (stopWatch != null) stopWatch.stopNow();

            if (keyInfoRedis != null) {
                LOGGER.debug("found from redis");
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                keyInfo.copy(keyInfoRedis);
                return true;
            }
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "info"));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null || dbPair.getData() == null) {
            LOGGER.debug("keyinfo not found from database for " + key);
            return false;
        }
        LOGGER.debug("found key info from database for " + key);

        keyInfo.fromMap(dbPair.getData());

        if (enableLocalCache) {
            AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
        }

        if (enableRedisCache) {
            Utils.getExcutorService().submit(() -> {
                Thread.yield();
                StopWatch stopWatch2 = context.startStopWatch("redis", "keyInfoOps.put");
                keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfo);
                if (stopWatch2 != null) stopWatch2.stopNow();
            });
        }

        return true;
    }

    @Override
    public boolean find(Context context, List<KeyInfo> keyInfos) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("find: " + pairs.size());

        if (pairs.size() == 0) {
            return false;
        }

        List<String> keys = new ArrayList<String>();
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
            return true;
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
                return true;
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
            Utils.getExcutorService().submit(() -> {

                Thread.yield();

                StopWatch stopWatch2 = context.startStopWatch("dbase", "finalQuery.save");
                keyInfoOps.putAll(hkeyPrefix + "::keyinfo", redisKeyInfoMap);
                if (stopWatch2 != null) stopWatch2.stopNow();

            });
        }
        return true;
    }

    @Override
    public boolean save(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();

        LOGGER.trace("save: " + key + " table: " + keyInfo.getTable());

        if (!keyInfo.getIsNew()) {
            LOGGER.warn("save KeyInfo is not new");
            return true;
        }

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
                if (!Query.hasStdClause(context, keyInfo) && Query.hasStdClause(context, cachedKeyInfo)) {
                    keyInfo.setClause(cachedKeyInfo.getClause());
                    keyInfo.setParams(cachedKeyInfo.getParams());
                }
                if (cachedKeyInfo.equals(keyInfo)) {
                    keyInfo.setIsNew(false);
                    return true;
                }
            }
            AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
        }

        final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
        if (finalQueryInfo != null) {
            keyInfo.setQueryInfo(null);
            Utils.getExcutorService().submit(() -> {

                Thread.yield();
                Query.save(context, finalQueryInfo);
            });
        }

        keyInfo.setIsNew(false);

        Utils.getExcutorService().submit(() -> {

            Thread.yield();

            StopWatch stopWatch = null;

            if (enableRedisCache) {

                stopWatch = context.startStopWatch("redis", "keyInfoOps.put");
                keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfo);
                if (stopWatch != null) stopWatch.stopNow();
            }

            KvPair dbPair = new KvPair(key, "info", keyInfo.toMap());

            stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(dbPair);
            if (stopWatch != null) stopWatch.stopNow();
        });

        return true;
    }

    @Override
    public boolean save(Context context, List<KeyInfo> keyInfos) {

        List<KvPair> pairs = context.getPairs();

        LOGGER.trace("save: " + pairs.size() + " keyInfos: " + keyInfos.size());

        if (pairs.size() == 0 || keyInfos.size() == 0) {
            return false;
        }

        List<String> todoKeys = new ArrayList<String>();
        List<KeyInfo> todoKeyInfos = new ArrayList<KeyInfo>();

        int i = 0;
        for (KvPair pair: pairs) {

            KeyInfo keyInfo = keyInfos.get(i++);

            if (!keyInfo.getIsNew()) {
                LOGGER.warn("save KeyInfo is not new");
                continue;
            }

            String key = pair.getId();

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
                    if (!Query.hasStdClause(context, keyInfo) && Query.hasStdClause(context, cachedKeyInfo)) {
                        keyInfo.setClause(cachedKeyInfo.getClause());
                        keyInfo.setParams(cachedKeyInfo.getParams());
                    }
                    if (cachedKeyInfo.equals(keyInfo)) {
                        keyInfo.setIsNew(false);
                        continue;
                    }
                }
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            }
            todoKeys.add(key);
            todoKeyInfos.add(keyInfo);
        }
        if (todoKeys.size() == 0) {
            return true;
        }

        for (KeyInfo keyInfo: todoKeyInfos) {
            keyInfo.setIsNew(false);
        }

        Utils.getExcutorService().submit(() -> {

            Thread.yield();

            if (enableRedisCache) {
                int index = 0;
                for (String key : todoKeys) {
                    KeyInfo keyInfoPer = todoKeyInfos.get(index++);

                    StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.putAll");
                    keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfoPer);
                    if (stopWatch != null) stopWatch.stopNow();
                }
            }

            List<KvPair> dbPairs = new ArrayList<KvPair>();
            int index = 0;
            for (String key: todoKeys) {
                dbPairs.add(new KvPair(key, "info", todoKeyInfos.get(index++).toMap()));
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(dbPairs);
            if (stopWatch != null) stopWatch.stopNow();
        });

        return true;
    }

    private void deleteOne(Context context, boolean dbOps) {
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
    public void delete(Context context, boolean dbOps) {

        List<KvPair> pairs = context.getPairs();

        if (pairs.size() == 1) {
            deleteOne(context, dbOps);
            return;
        }

        LOGGER.trace("delete: " + pairs.size());

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