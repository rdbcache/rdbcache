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
import com.rdbcache.repositories.KeyInfoRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

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
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("find pairs(" + pairs.size() + ") anyKey(" + anyKey.size() + ")");

        if (pairs.size() == 0 || pairs.getPair() == null) {
            LOGGER.error("find(0) failed - nothing to find");
            return false;
        }

        LOGGER.trace("key: " + pairs.getPair().getId() + (pairs.size() == 1 ? "" : "..."));

        List<String> keys = new ArrayList<String>();
        List<String> redisKeys = new ArrayList<String>();
        List<KvIdType> idTypes = new ArrayList<KvIdType>();

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            if (i == anyKey.size()) {
                anyKey.add(new KeyInfo());
            }

            KvPair pair = pairs.get(i);
            String key =  pair.getId();
            keys.add(key);

            KeyInfo keyInfo = null;
            if (enableLocalCache) {
                keyInfo = AppCtx.getLocalCache().getKeyInfo(key);
            }

            if (keyInfo == null) {
                foundAll = false;
                if (enableRedisCache) {
                    redisKeys.add(key);
                } else {
                    idTypes.add(new KvIdType(key, "info"));
                }
            } else {
                anyKey.set(i, keyInfo);
                keys.set(i, null);
            }
        }

        if (foundAll) {
            LOGGER.debug("find(" + pairs.size() + ") Ok - found from cache");
            return true;
        }


        if (redisKeys.size() > 0) {

            foundAll = true;

            if (redisKeys.size() == 1) {

                String key = redisKeys.get(0);

                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.get");
                KeyInfo keyInfoRedis = keyInfoOps.get(hkeyPrefix + "::keyinfo", key);
                if (stopWatch != null) stopWatch.stopNow();

                if (keyInfoRedis == null) {
                    foundAll = false;
                    idTypes.add(new KvIdType(key, "info"));
                } else {

                    if (enableLocalCache) {
                        AppCtx.getLocalCache().putKeyInfo(key, keyInfoRedis);
                    }
                    int index = keys.indexOf(key);
                    anyKey.set(index, keyInfoRedis);
                    keys.set(index, null);
                }
            } else {

                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.multiGet");
                List<KeyInfo> redisKeyInfos = keyInfoOps.multiGet(hkeyPrefix + "::keyinfo", redisKeys);
                if (stopWatch != null) stopWatch.stopNow();

                for (int i = 0; i < redisKeys.size(); i++) {

                    String key = redisKeys.get(i);
                    KeyInfo keyInfo = redisKeyInfos.get(i);

                    if (keyInfo == null) {
                        foundAll = false;
                        idTypes.add(new KvIdType(key, "info"));
                    } else {

                        if (enableLocalCache) {
                            AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                        }
                        int index = keys.indexOf(key);
                        anyKey.setKey(index, keyInfo);
                        keys.set(index, null);
                    }
                }
            }

            if (foundAll) {
                LOGGER.debug("find(" + pairs.size() + ") Ok - found from redis");
                return true;
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "kvPairRepo.findAll");
        Iterable<KvPair> dbPairs = AppCtx.getKvPairRepo().findAll(idTypes);
        if (stopWatch != null) stopWatch.stopNow();

        Map<String, KeyInfo> redisKeyInfoMap = new LinkedHashMap<String, KeyInfo>();

        for (KvPair dbPair: dbPairs) {

            if (dbPair == null) continue;

            String key = dbPair.getId();
            KeyInfo keyInfo = new KeyInfo(dbPair.getData());

            int index = keys.indexOf(key);
            anyKey.setKey(index, keyInfo);
            keys.set(index, null);

            if (enableLocalCache) {
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            }
            if (enableRedisCache) {
                redisKeyInfoMap.put(key, keyInfo);
            }
        }

        if (redisKeyInfoMap.size() > 0) {
            Utils.getExcutorService().submit(() -> {

                StopWatch stopWatch2 = context.startStopWatch("dbase", "finalQuery.save");
                keyInfoOps.putAll(hkeyPrefix + "::keyinfo", redisKeyInfoMap);
                if (stopWatch2 != null) stopWatch2.stopNow();

            });
        }

        foundAll = true;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) != null) {
                foundAll = false;
                break;
            }
        }

        if (foundAll) {
            LOGGER.debug("find(" + pairs.size() + ") Ok - found from database");
        } else {
            LOGGER.debug("find(" + pairs.size() + ") not found");
        }

        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        Assert.isTrue(anyKey.size() == pairs.size(), anyKey.size() + " != " +
                pairs.size() + ", save only supports anyKey size == pairs size");

        if (pairs.size() == 0 || anyKey.size() == 0) {
            LOGGER.debug("save(" + pairs.size() + ") anyKey(" + anyKey.size() + ") - nothing to save");
            return false;
        }

        LOGGER.trace("save(" + pairs.size() + "): " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") + anyKey.getAny().toString());

        List<String> todoKeys = new ArrayList<String>();
        List<KeyInfo> todoKeyInfos = new ArrayList<KeyInfo>();

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);

            String key = pair.getId();

            if (key == null || key.length() == 0) {
                LOGGER.trace("save(" + key + ") failed - invalid key");
                continue;
            }

            if (enableLocalCache) {
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
            }

            if (!keyInfo.getIsNew()) {
                LOGGER.trace("save KeyInfo is not new, skip ...");
                continue;
            }

            todoKeys.add(key);
            todoKeyInfos.add(keyInfo);
        }
        if (todoKeys.size() == 0) {
            LOGGER.debug("save(" + pairs.size() + ") OK");
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

        LOGGER.debug("save(" + pairs.size() + ") OK");

        return true;
    }

    @Override
    public void delete(Context context, KvPairs pairs, boolean dbOps) {

        if (pairs.size() == 0) {
            LOGGER.warn("delete(" + pairs.size() + ")");
            return;
        }

        LOGGER.trace("delete(" + pairs.size() + "): " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));

        if (enableLocalCache) {
            AppCtx.getLocalCache().removeKeyInfo(pairs);
        }

        if (enableRedisCache) {
            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
            if (pairs.size() == 1) {
                keyInfoOps.delete(hkeyPrefix + "::keyinfo", pairs.getPair().getId());
            } else {
                keyInfoOps.delete(hkeyPrefix + "::keyinfo", pairs.getKeys());
            }
            if (stopWatch != null) stopWatch.stopNow();
        }

        if (dbOps) {
            AppCtx.getKvPairRepo().delete(pairs);
        }
        LOGGER.debug("delete(" + pairs.size() + ") Ok");
    }
}