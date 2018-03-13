/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import com.rdbcache.repositories.RedisRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class RedisRepoImpl implements RedisRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisRepoImpl.class);

    private boolean enableLocalCache = true;

    private String hdataPrefix = PropCfg.getHdataPrefix();
    
    private String eventPrefix = PropCfg.getEventPrefix();
    
    @Autowired
    private StringRedisTemplate redisTemplate;

    private HashOperations hashOps;

    @PostConstruct
    public void init() {
        hashOps = redisTemplate.opsForHash();
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        hdataPrefix = PropCfg.getHdataPrefix();
        eventPrefix = PropCfg.getEventPrefix();
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

    public String getHdataPrefix() {
        return hdataPrefix;
    }

    public void setHdataPrefix(String hdataPrefix) {
        this.hdataPrefix = hdataPrefix;
    }

    public String getEventPrefix() {
        return eventPrefix;
    }

    public void setEventPrefix(String eventPrefix) {
        this.eventPrefix = eventPrefix;
    }

    @Override
    public boolean ifExits(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();
        String table = anyKey.getKey().getTable();

        LOGGER.trace("ifExits: " + key + " table: " + table);

        if (enableLocalCache) {
            if (AppCtx.getLocalCache().containsData(key)) {
                return true;
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
        boolean result = AppCtx.getRedisTemplate().hasKey(hdataPrefix + "::" + key);
        if (stopWatch != null) stopWatch.stopNow();

        return result;
    }

    private boolean findOne(Context context, KvPair pair, KeyInfo keyInfo) {

        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("findOne: " + key + " table: " + table);

        String hashKey = hdataPrefix + "::" + key;
        Map<String, Object> map = null;

        if (enableLocalCache) {
            map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
        }

        if (map == null) {

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
            try {
                map = hashOps.entries(hashKey);
                if (stopWatch != null) stopWatch.stopNow();

                if (enableLocalCache) {
                    AppCtx.getLocalCache().putData(key, map, keyInfo);
                }
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                throw new ServerErrorException(context, msg);
            }
        }

        if (map == null || map.size() == 0) {
            LOGGER.debug(hashKey + " not found from redis");
            return false;
        }

        pair.setData(map);
        return true;
    }

    private boolean saveOne(Context context, KvPair pair, KeyInfo keyInfo) {

        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("saveOne: " + key + " table: " + table);

        String hashKey = hdataPrefix + "::" + key;
        Map<String, Object> map = pair.getData();

        if (enableLocalCache) {
            AppCtx.getLocalCache().putData(key, map, keyInfo);
        }

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            if (enableLocalCache) {
                AppCtx.getLocalCache().removeData(key);
            }

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }
    }

    @Override
    public boolean updateIfExists(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();
        String table = anyKey.getKey().getTable();

        LOGGER.trace("updateIfExists: " + key + " table: " + table);

        String hashKey = hdataPrefix + "::" + key;

        StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
        boolean result = AppCtx.getRedisTemplate().hasKey(hashKey);
        if (stopWatch != null) stopWatch.stopNow();

        if (!result) return false;

        if (enableLocalCache) {
            AppCtx.getLocalCache().updatePairsData(pairs, anyKey);
        }

        Map<String, Object> map = pair.getData();

        stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            if (enableLocalCache) {
                AppCtx.getLocalCache().removeData(key);
            }

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {


        if (pairs.size() == 1) {
            return findOne(context, pairs.getPair(), anyKey.getKey());
        }

        String table = anyKey.getKey().getTable();

        LOGGER.trace("find: " + pairs.size() + " table: " + table);

        int i = 0;
        boolean foundAll = true;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            String hashKey = hdataPrefix + "::" + key;
            Map<String, Object> map = null;

            if (enableLocalCache) {
                map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
            }

            if (map == null) {

                StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
                try {
                    map = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (enableLocalCache) {
                        AppCtx.getLocalCache().putData(key, map, anyKey.getAny(i++));
                    }

                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                    e.printStackTrace();
                }
            }

            if (map == null || map.size() == 0) {
                LOGGER.debug(hashKey + " not found from redis");
                foundAll = false;
                continue;
            }

            pair.setData(map);
        }
        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        if (pairs.size() == 1) {
            return saveOne(context, pairs.getPair(), anyKey.getKey());
        }

        String table = anyKey.getKey().getTable();

        LOGGER.trace("save: " + pairs.size() + " table: " + table);

        int i = 0;
        boolean savedAll = true;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            String hashKey = hdataPrefix + "::" + key;
            Map<String, Object> map = pair.getData();

            if (enableLocalCache) {
                AppCtx.getLocalCache().putData(key, map, anyKey.getAny(i++));
            }

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableLocalCache) {
                    AppCtx.getLocalCache().removeData(key);
                }

                savedAll = false;
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }
        }
        return savedAll;
    }

    @Override
    public boolean findAndSave(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();
        String table =  anyKey.getKey().getTable();

        LOGGER.trace("findAndSave: " + key + " table: " + table);

        String hashKey = hdataPrefix + "::" + key;
        Map<String, Object> map = pair.getData();
        Map<String, Object> fmap = null;

        if (enableLocalCache) {
            fmap = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
        }

        StopWatch stopWatch = null;

        if (fmap == null) {
            try {
                stopWatch = context.startStopWatch("redis", "hashOps.entries");
                fmap = hashOps.entries(hashKey);
                if (stopWatch != null) stopWatch.stopNow();
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                throw new ServerErrorException(context, msg);
            }
        }

        try {
            if (enableLocalCache) {
                AppCtx.getLocalCache().putData(key, map, anyKey.getKey());
            }

            stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();

        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            if (enableLocalCache) {
                AppCtx.getLocalCache().removeData(key);
            }

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }

        if (fmap != null && fmap.size() > 0) {
            pair.setData(fmap);
            return true;
        } else {
            LOGGER.debug(hashKey + " not found from redis");
            return false;
        }
    }

    @Override
    public void delete(Context context, KvPairs pairs, AnyKey anyKey, boolean dbOps) {

        LOGGER.trace("delete: " + pairs.size() + " table: " + anyKey.getAny().getTable());

        if (enableLocalCache) {
            AppCtx.getLocalCache().removeKeyAndData(pairs);
        }

        List<String> hashKeys = new ArrayList<>();
        for (KvPair pair: pairs) {

            String key = pair.getId();
            hashKeys.add(hdataPrefix + "::" + key);

            LOGGER.trace("delete: " + key);
        }

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.delete");
        AppCtx.getRedisTemplate().delete(hashKeys);
        if (stopWatch != null) stopWatch.stopNow();

        deleteExpireEvents(context, pairs, anyKey);

        AppCtx.getKeyInfoRepo().delete(context, pairs, dbOps);
    }

    private void deleteExpireEvents(Context context, KvPairs pairs, AnyKey anyKey) {

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);

            String key = pair.getId();
            String table = keyInfo.getTable();

            LOGGER.debug("deleteExpireEvents: " + key + " table: " + table);

            // get existing expire key
            String expKey = eventPrefix + "::" + key;

            StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
            Set<String> expKeys = AppCtx.getRedisTemplate().keys(expKey + "::*");
            if (stopWatch != null) stopWatch.stopNow();

            if (expKeys != null && expKeys.size() > 0) {
                stopWatch = context.startStopWatch("redis", "redisTemplate.delete");
                AppCtx.getRedisTemplate().delete(expKeys);
                if (stopWatch != null) stopWatch.stopNow();
            }
        }
    }
}
