/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import com.rdbcache.repositories.RedisRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
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

    private boolean enableDataCache = true;

    private String hdataPrefix = PropCfg.getHdataPrefix();

    private String eventPrefix = PropCfg.getEventPrefix();
    
    private StringRedisTemplate stringRedisTemplate;

    private HashOperations hashOps;

    @PostConstruct
    public void init() {
        //System.out.println("*** init RedisRepoImpl");
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        hdataPrefix = PropCfg.getHdataPrefix();
        eventPrefix = PropCfg.getEventPrefix();
        if (PropCfg.getDataMaxCacheTLL() <= 0l) {
            enableDataCache = false;
        } else {
            enableDataCache = true;
        }
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {

        StringRedisTemplate stringRedisTemplate = AppCtx.getStringRedisTemplate();
        if (stringRedisTemplate == null) {
            LOGGER.error("failed to get redis template");
            return;
        }
        hashOps = stringRedisTemplate.opsForHash();
    }

    public boolean isEnableDataCache() {
        return enableDataCache;
    }

    public void setEnableDataCache(boolean enable) {
        this.enableDataCache = enable;
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

        LOGGER.trace("ifExist pairs(" + pairs.size() + "): " + pairs.printKey() + 
            "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();

            if (enableDataCache) {
                if (AppCtx.getLocalCache().containsData(key)) {
                    LOGGER.trace("ifExist found from cache " + key);
                    continue;
                }
            }

            StopWatch stopWatch = context.startStopWatch("redis", "stringRedisTemplate.hasKey");
            foundAll = AppCtx.getStringRedisTemplate().hasKey(hdataPrefix + "::" + key);
            if (stopWatch != null) stopWatch.stopNow();

            if (!foundAll) {
                LOGGER.debug("ifExit not found from redis " + key);
                break;
            } else {
                LOGGER.debug("ifExit found redis " + key);
            }
        }

        LOGGER.debug("ifExist returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean update(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("update pairs(" + pairs.size() + "): " + pairs.printKey() +
            "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {
            
            KvPair pair = pairs.get(i);

            if (enableDataCache) {
                AppCtx.getLocalCache().updateData(pair);
            }

            String key = pair.getId();
            String hashKey = hdataPrefix + "::" + key;
            Map<String, Object> map = pair.getData();

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("update redis for " + key);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                foundAll = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                throw new ServerErrorException(context, msg);
            }
        }

        LOGGER.debug("update returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("find pairs(" + pairs.size() + "): " + pairs.printKey() + 
            "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + key;
            Map<String, Object> map = null;

            if (enableDataCache) {
                map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);
                if (map != null && map.size() > 0) {
                    LOGGER.debug("find - found from cache " + key);
                }
            }

            if (map == null) {

                StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
                try {
                    map = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (map != null && map.size() > 0) {

                        if (enableDataCache) {
                            AppCtx.getLocalCache().putData(pair, keyInfo);
                        }

                        LOGGER.debug("find - found from redis " + key);
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
                LOGGER.debug("find - not found " + key);
                continue;
            }

            pair.setData(map);
        }

        LOGGER.trace("find returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("save pairs(" + pairs.size() + "): " + pairs.printKey() + 
            "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        boolean savedAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();

            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + key;

            Map<String, Object> map = pair.getData();
            if (enableDataCache) {
                AppCtx.getLocalCache().putData(pair, keyInfo);
            }

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("save to redis for " + key);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableDataCache) {
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

        LOGGER.trace("findAndSave pairs(" + pairs.size() + "): " + pairs.printKey() + 
            "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + key;

            Map<String, Object> map = pair.getData();
            Map<String, Object> fmap = null;

            if (enableDataCache) {
                fmap = AppCtx.getLocalCache().getData(key);
                if (fmap != null && fmap.size() > 0) {
                    LOGGER.debug("findAndSave - found from cache " + key);
                }
            }

            StopWatch stopWatch = null;

            if (fmap == null) {
                try {
                    stopWatch = context.startStopWatch("redis", "hashOps.entries");
                    fmap = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (fmap != null && fmap.size() > 0) {
                        LOGGER.debug("findAndSave - found from redis " + key);
                    }
                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    e.printStackTrace();
                    throw new ServerErrorException(context, msg);
                }
            }

            if (enableDataCache) {
                AppCtx.getLocalCache().putData(pair, keyInfo);
            }

            try {
                stopWatch = context.startStopWatch("redis", "hashOps.putAll");
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("findAndSave - save " + key);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableDataCache) {
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

        LOGGER.trace("findAndSave returns " + allOk);

        return allOk;
    }

    @Override
    public void delete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("findAndSave pairs(" + pairs.size() + "): " + pairs.printKey() +
                "anyKey(" + anyKey.size() + "): " + anyKey.printTable());

        if (enableDataCache) {
            AppCtx.getLocalCache().removeKeyAndData(pairs);
        }

        Set<String> hashKeys = new HashSet<>();
        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            hashKeys.add(hdataPrefix + "::" + key);

            String expKey = eventPrefix + "::" + key;

            // get existing expire key
            StopWatch stopWatch = context.startStopWatch("redis", "stringRedisTemplate.hasKey");
            Set<String> expKeys = AppCtx.getStringRedisTemplate().keys(expKey + "::*");
            if (stopWatch != null) stopWatch.stopNow();

            if (expKeys != null && expKeys.size() > 0) {
                stopWatch = context.startStopWatch("redis", "stringRedisTemplate.delete");
                AppCtx.getStringRedisTemplate().delete(expKeys);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("delete " + key);
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "stringRedisTemplate.delete");
        AppCtx.getStringRedisTemplate().delete(hashKeys);
        if (stopWatch != null) stopWatch.stopNow();

        LOGGER.trace("delete done");

    }
}
