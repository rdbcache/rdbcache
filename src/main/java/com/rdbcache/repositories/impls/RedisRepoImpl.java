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
    public boolean ifExist(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("ifExist pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String table = anyKey.getKey(i).getTable();

            if (enableLocalCache) {
                if (AppCtx.getLocalCache().containsData(key)) {
                    LOGGER.trace("ifExist found " + key + " from cache");
                    continue;
                }
            }

            StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
            foundAll = AppCtx.getRedisTemplate().hasKey(hdataPrefix + "::" + key);
            if (stopWatch != null) stopWatch.stopNow();

            if (!foundAll) {
                break;
            }
        }

        LOGGER.debug("ifExist returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean updateIfExist(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("updateIfExist pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {
            
            KvPair pair = pairs.get(i);
            String key = pair.getId();

            boolean found = false;
            if (enableLocalCache) {
                if (AppCtx.getLocalCache().containsData(key)) {
                    found = true;
                    LOGGER.trace("updateIfExist found " + key + " from cache");
                }
            }

            String hashKey = hdataPrefix + "::" + key;

            if (!found) {

                StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
                boolean result = AppCtx.getRedisTemplate().hasKey(hashKey);
                if (stopWatch != null) stopWatch.stopNow();

                if (!result) {
                    foundAll = false;
                    LOGGER.trace("updateIfExist can not find " + key + " from redis");
                    continue;
                }
            }

            if (enableLocalCache) {
                AppCtx.getLocalCache().updatePairsData(pairs, anyKey);
            }

            Map<String, Object> map = pair.getData();

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();
                return true;
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                foundAll = false;

                if (enableLocalCache) {
                    AppCtx.getLocalCache().removeData(key);
                }

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                throw new ServerErrorException(context, msg);
            }
        }

        LOGGER.debug("updateIfExist returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("find pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();

            String hashKey = hdataPrefix + "::" + key;
            Map<String, Object> map = null;

            if (enableLocalCache) {
                map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
                if (map != null && map.size() > 0) {
                    LOGGER.trace("find - found " + key + " from cache");
                }
            }

            if (map == null) {

                StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
                try {
                    map = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (map != null && map.size() > 0) {
                        LOGGER.trace("find - found " + key + " from redis");
                        if (enableLocalCache) {
                            AppCtx.getLocalCache().putData(key, map, anyKey.getAny());
                        }
                    }
                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    foundAll = false;

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                    e.printStackTrace();
                    continue;
                }
            }

            if (map == null || map.size() == 0) {
                foundAll = false;
                LOGGER.trace("find - not found " + key);
                continue;
            }

            pair.setData(map);
        }

        LOGGER.debug("find returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("save pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        boolean savedAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + key;

            Map<String, Object> map = pair.getData();
            if (enableLocalCache) {
                AppCtx.getLocalCache().putData(key, map, keyInfo);
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

        LOGGER.trace("save returns " + savedAll);

        return savedAll;
    }

    @Override
    public boolean findAndSave(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("findAndSave pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + key;

            Map<String, Object> map = pair.getData();
            Map<String, Object> fmap = null;

            if (enableLocalCache) {
                fmap = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
                if (fmap != null && fmap.size() > 0) {
                    LOGGER.trace("findAndSave - found " + key + " from cache");
                }
            }

            StopWatch stopWatch = null;

            if (fmap == null) {
                try {
                    stopWatch = context.startStopWatch("redis", "hashOps.entries");
                    fmap = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (fmap != null && fmap.size() > 0) {
                        LOGGER.trace("findAndSave - found " + key + " from redis");
                    }
                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    e.printStackTrace();
                    throw new ServerErrorException(context, msg);
                }
            }

            if (enableLocalCache) {
                AppCtx.getLocalCache().putData(key, map, keyInfo);
            }

            try {
                stopWatch = context.startStopWatch("redis", "hashOps.putAll");
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableLocalCache) {
                    AppCtx.getLocalCache().removeData(key);
                }

                allOk = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }

            if (fmap != null && fmap.size() > 0) {
                if (allOk) pair.setData(fmap);
            } else {
                allOk = false;
            }
        }

        LOGGER.debug("findAndSave returns " + allOk);

        return allOk;
    }

    @Override
    public void delete(Context context, KvPairs pairs, AnyKey anyKey, boolean dbOps) {

        LOGGER.trace("findAndSave pairs(" + pairs.size() + "): "+ pairs.getPair().getId() +
                (pairs.size() > 1 ? " ... " : " ") +
                "anyKey(" + anyKey.size() + "): " + anyKey.getAny().getTable());

        if (enableLocalCache) {
            AppCtx.getLocalCache().removeKeyAndData(pairs);
        }

        List<String> hashKeys = new ArrayList<>();
        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            hashKeys.add(hdataPrefix + "::" + key);

            String expKey = eventPrefix + "::" + key;

            // get existing expire key
            StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
            Set<String> expKeys = AppCtx.getRedisTemplate().keys(expKey + "::*");
            if (stopWatch != null) stopWatch.stopNow();

            if (expKeys != null && expKeys.size() > 0) {
                stopWatch = context.startStopWatch("redis", "redisTemplate.delete");
                AppCtx.getRedisTemplate().delete(expKeys);
                if (stopWatch != null) stopWatch.stopNow();
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.delete");
        AppCtx.getRedisTemplate().delete(hashKeys);
        if (stopWatch != null) stopWatch.stopNow();

        AppCtx.getKeyInfoRepo().delete(context, pairs, dbOps);

        LOGGER.debug("delete done");

    }
}
