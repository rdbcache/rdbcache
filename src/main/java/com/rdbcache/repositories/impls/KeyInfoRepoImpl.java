/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.configs.KeyInfoRedisTemplate;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.helpers.*;
import com.rdbcache.models.*;
import com.rdbcache.repositories.KeyInfoRepo;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class KeyInfoRepoImpl implements KeyInfoRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyInfoRepoImpl.class);

    private boolean enableRedisCache = true;

    private String hkeyPrefix = PropCfg.getHkeyPrefix();

    private HashOperations<String, String, KeyInfo> keyInfoOps;

    @PostConstruct
    public void init() {
        //System.out.println("*** init KeyInfoRepoImpl");
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        hkeyPrefix = PropCfg.getHkeyPrefix();
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {

        KeyInfoRedisTemplate template = AppCtx.getKeyInfoRedisTemplate();
        if (template == null) {
            LOGGER.error("failed to get key info redis template");
            return;
        }
        keyInfoOps = template.opsForHash();
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
    public boolean find(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        LOGGER.trace("find " + pair.printKey() + " " + keyInfo.toString());

        boolean foundAll = true;

        String key = pair.getId();

        if (!pair.isNewUuid()) {

            KeyInfo keyInfoCached = AppCtx.getLocalCache().getKeyInfo(key);

            if (keyInfoCached != null) {
                keyInfo.copy(keyInfoCached);
                LOGGER.trace("find - found from cache: " + key);
                return true;
            }

            if (enableRedisCache) {
                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.get");
                KeyInfo keyInfoRedis = keyInfoOps.get(hkeyPrefix + "::keyinfo", key);
                if (stopWatch != null) stopWatch.stopNow();

                if (keyInfoRedis != null) {
                    keyInfo.copy(keyInfoCached);
                    AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                    LOGGER.trace("find - found from cache: " + key);
                    return true;
                }
            }
        }

        KvIdType idType = new KvIdType(key, "info");

        StopWatch stopWatch = context.startStopWatch("redis", "kvPairRepo.findAll");
        Optional<KvPair> dbPairOpt = AppCtx.getKvPairRepo().findById(idType);
        if (stopWatch != null) stopWatch.stopNow();

        if (!dbPairOpt.isPresent()) {
            LOGGER.debug("find - not found from anywhere: " + key);
            return false;
        }

        KvPair dbPair = dbPairOpt.get();
        Map<String, Object> map = dbPair.getData();
        KeyInfo keyInfoDb = Utils.toPojo(map, KeyInfo.class);

        keyInfo.copy(keyInfoDb);
        AppCtx.getLocalCache().putKeyInfo(key, keyInfo);

        Utils.getExcutorService().submit(() -> {
            StopWatch stopWatch2 = context.startStopWatch("dbase", "finalQuery.save");
            keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfo);
            if (stopWatch2 != null) stopWatch2.stopNow();
        });

        return true;
    }

    @Override
    public boolean find(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        LOGGER.trace("find " + pairs.printKey() + "anyKey(" + anyKey.size() + ")");

        List<String> keys = new ArrayList<String>();
        List<String> redisKeys = new ArrayList<String>();
        List<KvIdType> idTypes = new ArrayList<KvIdType>();

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();

            if (i < anyKey.size()) {
                // already has it, no need to find
                keys.add(null);
                LOGGER.trace("find - already existed: " + key);
                continue;
            } else {
                keys.add(key);
            }

            KeyInfo keyInfo = null;
            if (!pair.isNewUuid()) {
                keyInfo = AppCtx.getLocalCache().getKeyInfo(key);
            }

            if (keyInfo != null) {
                anyKey.add(keyInfo);
                keys.set(i, null);
                LOGGER.trace("find - found from cache: " + key);
                continue;
            }

            keyInfo = anyKey.getAny(i);

            Assert.isTrue(keyInfo.getIsNew(), "anyKey.getAny(" + i + ") return keyInfo should be new");

            if (!pair.isNewUuid()) {
                foundAll = false;
                if (enableRedisCache) {
                    redisKeys.add(key);
                } else {
                    idTypes.add(new KvIdType(key, "info"));
                }
            } else {
                // new generated uuid - skip finding it
                keys.set(i, null);
                LOGGER.trace("find - new uuid skipped: " + key);
            }
        }

        if (foundAll) {
            LOGGER.debug("find - found from cache");
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
                    KeyInfo keyInfo = AppCtx.getLocalCache().putKeyInfo(key, keyInfoRedis);
                    int index = keys.indexOf(key);
                    anyKey.set(index, keyInfo);
                    keys.set(index, null);
                    LOGGER.trace("find - found from redis: " + key);
                }
            } else {

                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.multiGet");
                List<KeyInfo> redisKeyInfos = keyInfoOps.multiGet(hkeyPrefix + "::keyinfo", redisKeys);
                if (stopWatch != null) stopWatch.stopNow();

                for (int i = 0; i < redisKeys.size() && i < redisKeyInfos.size(); i++) {

                    String key = redisKeys.get(i);
                    KeyInfo keyInfo = redisKeyInfos.get(i);

                    if (keyInfo == null) {
                        foundAll = false;
                        idTypes.add(new KvIdType(key, "info"));
                    } else {
                        keyInfo = AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                        int index = keys.indexOf(key);
                        anyKey.set(index, keyInfo);
                        keys.set(index, null);
                        LOGGER.trace("find - found from redis: " + key);
                    }
                }
            }

            if (foundAll) {
                LOGGER.debug("find - found from redis");
                return true;
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "kvPairRepo.findAll");
        Iterable<KvPair> dbPairs = AppCtx.getKvPairRepo().findAllById(idTypes);
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPairs.iterator().hasNext()) {

            Map<String, KeyInfo> redisKeyInfoMap = new LinkedHashMap<String, KeyInfo>();

            for (KvPair dbPair : dbPairs) {

                if (dbPair == null) {
                    continue;
                }

                LOGGER.trace("find - found from database: " + dbPair.printKey());

                String key = dbPair.getId();
                KeyInfo keyInfo = Utils.toPojo(dbPair.getData(), KeyInfo.class);

                int index = keys.indexOf(key);
                anyKey.set(index, keyInfo);
                keys.set(index, null);

                keyInfo = AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
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
        }

        foundAll = true;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) != null) {
                foundAll = false;
                LOGGER.debug("find - not found from anywhere: " + keys.get(i));
                break;
            }
        }

        return foundAll;
    }

    @Override
    public boolean save(final Context context, final KvPair pair, final KeyInfo keyInfo) {


        LOGGER.trace("save " + pair.printKey() + " " + keyInfo.toString());

        String key = pair.getId();

        if (!keyInfo.getIsNew()) {
            LOGGER.trace("save KeyInfo is not new, skipped for " + key);
            return false;
        }

        keyInfo.cleanup();

        AppCtx.getLocalCache().putKeyInfo(key, keyInfo);

        if (enableRedisCache) {
            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.put");
            keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfo);
            if (stopWatch != null) stopWatch.stopNow();
        }

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        AppCtx.getKvPairRepo().save(new KvPair(key, "info", Utils.toMap(keyInfo)));
        if (stopWatch != null) stopWatch.stopNow();

        LOGGER.debug("save Ok: " + pair.printKey());

        return true;
    }

    @Override
    public boolean save(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        Assert.isTrue(anyKey.size() == pairs.size(), anyKey.size() + " != " +
                pairs.size() + ", only supports that pairs and anyKey have the same size");

        if (pairs.size() == 0 || anyKey.size() == 0) {
            LOGGER.debug("save " + pairs.printKey()  + "anyKey(" + anyKey.size() + ") - nothing to save");
            return false;
        }

        LOGGER.trace("save " + pairs.printKey() + anyKey.print());

        for (int i = 0; i < pairs.size() && i < anyKey.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.get(i);

            if (keyInfo == null) {
                LOGGER.trace("save KeyInfo is null for " + key);
                continue;
            }

            if (!keyInfo.getIsNew()) {
                LOGGER.trace("save KeyInfo is not new, skipped for " + key);
                continue;
            }

            keyInfo.cleanup();

            AppCtx.getLocalCache().putKeyInfo(key, keyInfo);

            if (enableRedisCache) {
                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.put");
                keyInfoOps.put(hkeyPrefix + "::keyinfo", key, keyInfo);
                if (stopWatch != null) stopWatch.stopNow();
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().save(new KvPair(key, "info", Utils.toMap(keyInfo)));
            if (stopWatch != null) stopWatch.stopNow();
        }

        LOGGER.debug("save Ok: " + pairs.printKey());

        return true;
    }

    @Override
    public void delete(final Context context, final KvPair pair) {

        LOGGER.trace("delete " + pair.printKey());

        AppCtx.getLocalCache().removeKeyInfo(pair.getId());

        if (enableRedisCache) {
            StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
            keyInfoOps.delete(hkeyPrefix + "::keyinfo", pair.getId());
            if (stopWatch != null) stopWatch.stopNow();
        }

        //intentional leave database not deleted

        LOGGER.debug("delete Ok: " + pair.printKey());
    }

    @Override
    public void delete(final Context context, final KvPairs pairs) {

        LOGGER.trace("delete " + pairs.printKey());

        AppCtx.getLocalCache().removeKeyInfo(pairs);

        if (enableRedisCache) {
            for (KvPair pair: pairs) {
                StopWatch stopWatch = context.startStopWatch("redis", "keyInfoOps.delete");
                keyInfoOps.delete(hkeyPrefix + "::keyinfo", pair.getId());
                if (stopWatch != null) stopWatch.stopNow();
            }
        }

        //intentional leave database not deleted

        LOGGER.debug("delete Ok: " + pairs.printKey());
    }
}