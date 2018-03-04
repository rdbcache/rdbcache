/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.implemnets;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import com.rdbcache.repositories.RedisRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class RedisRepoImpl implements RedisRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisRepoImpl.class);

    @Autowired
    private StringRedisTemplate redisTemplate;

    private HashOperations hashOps;

    @PostConstruct
    public void init() {
        hashOps = redisTemplate.opsForHash();
    }

    @Override
    public boolean ifExits(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("ifExits: " + key + " table: " + table);

        StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
        boolean result = AppCtx.getRedisTemplate().hasKey(Cfg.getHdataKey(key));
        if (stopWatch != null) stopWatch.stopNow();

        return result;
    }

    @Override
    public boolean findOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("findOne: " + key + " table: " + table);

        String hashKey = Cfg.getHdataKey(key);
        Map<String, Object> map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);

        if (map == null) {

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
            try {
                map = hashOps.entries(hashKey);
                if (stopWatch != null) stopWatch.stopNow();
                AppCtx.getLocalCache().putData(key, map, keyInfo);

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

    @Override
    public boolean saveOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("saveOne: " + key + " table: " + table);

        String hashKey = Cfg.getHdataKey(key);
        Map<String, Object> map = pair.getData();

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();
            AppCtx.getLocalCache().putData(key, map, keyInfo);
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }
    }

    @Override
    public boolean updateIfExists(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("updateIfExists: " + key + " table: " + table);

        String hashKey = Cfg.getHdataKey(key);

        StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
        boolean result = AppCtx.getRedisTemplate().hasKey(hashKey);
        if (stopWatch != null) stopWatch.stopNow();

        if (!result) return false;

        Map<String, Object> map = pair.getData();

        stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();
            AppCtx.getLocalCache().updateData(key, map, keyInfo);
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }
    }

    @Override
    public boolean findAll(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        String table = keyInfo.getTable();

        LOGGER.trace("findAll: " + pairs.size() + " table: " + table);

        boolean foundAll = true;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            String hashKey = Cfg.getHdataKey(key);
            Map<String, Object> map = (Map<String, Object>) AppCtx.getLocalCache().getData(key);

            if (map == null) {

                StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
                try {
                    map = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();
                    AppCtx.getLocalCache().putData(key, map, keyInfo);

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
    public boolean saveAll(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        String table = keyInfo.getTable();

        LOGGER.trace("saveAll: " + pairs.size() + " table: " + table);

        boolean savedAll = true;
        for (KvPair pair: pairs) {

            String key = pair.getId();
            String hashKey = Cfg.getHdataKey(key);
            Map<String, Object> map = pair.getData();

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();
                AppCtx.getLocalCache().putData(key, map, keyInfo);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

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
    public boolean findAndSave(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table =  keyInfo.getTable();

        LOGGER.trace("findAndSave: " + key + " table: " + table);

        String hashKey = Cfg.getHdataKey(key);
        Map<String, Object> map = pair.getData();
        Map<String, Object> fmap = (Map<String, Object>) AppCtx.getLocalCache().getData(key);

        StopWatch stopWatch = null;
        try {
            if (fmap == null) {
                stopWatch = context.startStopWatch("redis", "hashOps.entries");
                fmap = hashOps.entries(hashKey);
                if (stopWatch != null) stopWatch.stopNow();
            }

            stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();
            AppCtx.getLocalCache().putData(key, map, keyInfo);

            if (fmap != null && fmap.size() > 0) {
                pair.setData(fmap);
                return true;
            } else {
                LOGGER.debug(hashKey + " not found from redis");
                return false;
            }
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }
    }

    @Override
    public void delete(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("delete: " + key + " table: " + table);

        deleteExpireEvents(context, keyInfo);

        AppCtx.getKeyInfoRepo().deleteOne(context);

        AppCtx.getLocalCache().removeData(key);

        String hashKey = Cfg.getHdataKey(key);

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.delete");
        AppCtx.getRedisTemplate().delete(hashKey);
        if (stopWatch != null) stopWatch.stopNow();

    }

    @Override
    public void deleteOneCompletely(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("deleteOneCompletely: " + key + " table: " + table);

        delete(context, keyInfo);

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.delete");
        AppCtx.getKvPairRepo().delete(new KvIdType(key, "info"));
        if (stopWatch != null) stopWatch.stopNow();
    }

    private void deleteExpireEvents(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        // get existing expire key
        String expKey = Cfg.getEventPrefix() + "::" + key;

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
