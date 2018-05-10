/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.repositories.impls;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.KvPairs;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.models.StopWatch;
import doitincloud.rdbcache.repositories.RedisRepo;
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
    public boolean ifExist(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("ifExist: " + pair.printKey() + " " + keyInfo.toString());
        }

        String key = pair.getId();
        String type = pair.getType();
        String hashKey = hdataPrefix + "::" + type + ":" + key;

        if (enableDataCache) {
            if (AppCtx.getCacheOps().containsData(pair.getIdType())) {
                if (LOGGER.isTraceEnabled()) LOGGER.trace("ifExist found from cache " + key);
                return true;
            }
        }

        StopWatch stopWatch = context.startStopWatch("redis", "stringRedisTemplate.hasKey");
        boolean hasIt = AppCtx.getStringRedisTemplate().hasKey(hashKey);
        if (stopWatch != null) stopWatch.stopNow();

        if (!hasIt) {
            LOGGER.debug("ifExit not found from redis " + key);
            return false;
        } else {
            LOGGER.debug("ifExit found redis " + key);
            return true;
        }
    }

    @Override
    public boolean ifExist(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("ifExist pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String type = pair.getType();
            String hashKey = hdataPrefix + "::" + type + ":" + key;

            if (enableDataCache) {
                if (AppCtx.getCacheOps().containsData(pair.getIdType())) {
                    if (LOGGER.isTraceEnabled()) LOGGER.trace("ifExist found from cache " + key);
                    continue;
                }
            }

            StopWatch stopWatch = context.startStopWatch("redis", "stringRedisTemplate.hasKey");
            foundAll = AppCtx.getStringRedisTemplate().hasKey(hashKey);
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
    public boolean update(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("update: : " + pair.printKey() + " " + keyInfo.toString());
        }

        if (enableDataCache) {
            AppCtx.getCacheOps().updateData(pair);
        }

        String key = pair.getId();
        String type = pair.getType();
        String hashKey = hdataPrefix + "::" + type + ":" + key;
        Map<String, Object> map = pair.getData();

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();

            LOGGER.debug("update redis for " + key);

        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            throw new ServerErrorException(context, msg);
        }

        LOGGER.debug("update returns true");

        return true;
    }

    @Override
    public boolean update(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("update pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {
            
            KvPair pair = pairs.get(i);

            if (enableDataCache) {
                AppCtx.getCacheOps().updateData(pair);
            }

            String key = pair.getId();
            String type = pair.getType();
            String hashKey = hdataPrefix + "::" + type + ":" + key;
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
    public boolean find(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("find: " + pair.printKey() + " " + keyInfo.toString());
        }

        boolean foundAll = true;

        String key = pair.getId();
        String type = pair.getType();

        String hashKey = hdataPrefix + "::" + type + ":" + key;
        Map<String, Object> map = null;

        if (enableDataCache) {
            map = (Map<String, Object>) AppCtx.getCacheOps().getData(pair.getIdType());
            if (map != null && map.size() > 0) {
                pair.setData(map);
                LOGGER.debug("find - found from cache " + key);
            }
        }

        if (map == null) {

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
            try {
                map = hashOps.entries(hashKey);
                if (stopWatch != null) stopWatch.stopNow();

                if (map != null && map.size() > 0) {
                    pair.setData(map);
                    if (enableDataCache) {
                        AppCtx.getCacheOps().putData(pair, keyInfo);
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
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }
        }

        if (map == null || map.size() == 0) {
            foundAll = false;
            LOGGER.debug("find - not found " + key);
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("find returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean find(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("find pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        boolean foundAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String type = pair.getType();
            KeyInfo keyInfo = anyKey.getAny(i);

            String hashKey = hdataPrefix + "::" + type + ":" + key;
            Map<String, Object> map = null;

            if (enableDataCache) {
                map = (Map<String, Object>) AppCtx.getCacheOps().getData(pair.getIdType());
                if (map != null && map.size() > 0) {
                    pair.setData(map);
                    LOGGER.debug("find - found from cache " + key);
                }
            }

            if (map == null) {

                StopWatch stopWatch = context.startStopWatch("redis", "hashOps.entries");
                try {
                    map = hashOps.entries(hashKey);
                    if (stopWatch != null) stopWatch.stopNow();

                    if (map != null && map.size() > 0) {
                        pair.setData(map);
                        if (enableDataCache) {
                            AppCtx.getCacheOps().putData(pair, keyInfo);
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
                    if (context.isSync()) {
                        throw new ServerErrorException(context, msg);
                    }
                    continue;
                }
            }

            if (map == null || map.size() == 0) {
                foundAll = false;
                LOGGER.debug("find - not found " + key);
                continue;
            }
        }

        LOGGER.trace("find returns " + foundAll);

        return foundAll;
    }

    @Override
    public boolean save(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("save: " + pair.printKey() + " " + keyInfo.toString());
        }

        boolean savedAll = true;

        String key = pair.getId();
        String type = pair.getType();

        String hashKey = hdataPrefix + "::" + type + ":" + key;

        Map<String, Object> map = pair.getData();
        if (enableDataCache) {
            AppCtx.getCacheOps().putData(pair, keyInfo);
        }

        StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
        try {
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();

            LOGGER.debug("save to redis for " + key);

        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            if (enableDataCache) {
                AppCtx.getCacheOps().removeData(pair.getIdType());
            }

            savedAll = false;

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            context.logTraceMessage(msg);
            e.printStackTrace();
            if (context.isSync()) {
                throw new ServerErrorException(context, msg);
            }
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("save returns " + savedAll);

        return savedAll;
    }

    @Override
    public boolean save(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("save pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        boolean savedAll = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String type = pair.getType();
            String hashKey = hdataPrefix + "::" + type + ":" + key;
            KeyInfo keyInfo = anyKey.getAny(i);

            Map<String, Object> map = pair.getData();
            if (enableDataCache) {
                AppCtx.getCacheOps().putData(pair, keyInfo);
            }

            StopWatch stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            try {
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("save to redis for " + key);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableDataCache) {
                    AppCtx.getCacheOps().removeData(pair.getIdType());
                }

                savedAll = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("save returns " + savedAll);

        return savedAll;
    }

    @Override
    public boolean findAndSave(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("findAndSave: " + pair.printKey() + " " + keyInfo.toString());
        }

        boolean allOk = true;

        String key = pair.getId();
        String type = pair.getType();
        String hashKey = hdataPrefix + "::" + type + ":" + key;
        Map<String, Object> map = pair.getData();
        Map<String, Object> fmap = null;

        if (enableDataCache) {
            fmap = AppCtx.getCacheOps().getData(pair.getIdType());
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
            AppCtx.getCacheOps().putData(pair, keyInfo);
        }

        pair.setData(fmap);

        try {
            stopWatch = context.startStopWatch("redis", "hashOps.putAll");
            hashOps.putAll(hashKey, map);
            if (stopWatch != null) stopWatch.stopNow();

            LOGGER.debug("findAndSave - save " + key);

        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            if (enableDataCache) {
                AppCtx.getCacheOps().removeData(pair.getIdType());
            }

            allOk = false;

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
        }

        if (allOk) {
            allOk = fmap != null && fmap.size() > 0;
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("findAndSave returns " + allOk);

        return allOk;
    }

    @Override
    public boolean findAndSave(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("findAndSave pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String type = pair.getType();
            String hashKey = hdataPrefix + "::" + type + ":" + key;
            KeyInfo keyInfo = anyKey.getAny(i);

            Map<String, Object> map = pair.getData();
            Map<String, Object> fmap = null;

            if (enableDataCache) {
                fmap = AppCtx.getCacheOps().getData(pair.getIdType());
                if (fmap != null && fmap.size() > 0) {
                    LOGGER.debug("findAndSave - found from cache " + key);
                }
                AppCtx.getCacheOps().putData(pair, keyInfo);
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

            pair.setData(fmap);

            try {
                stopWatch = context.startStopWatch("redis", "hashOps.putAll");
                hashOps.putAll(hashKey, map);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.debug("findAndSave - save " + key);

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                if (enableDataCache) {
                    AppCtx.getCacheOps().removeData(pair.getIdType());
                }

                allOk = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }

            if (allOk) {
                allOk = fmap != null && fmap.size() > 0;
            }
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("findAndSave returns " + allOk);

        return allOk;
    }

    @Override
    public void delete(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("findAndSave: " + pair.printKey() + " " + keyInfo.toString());
        }

        if (enableDataCache) {
            AppCtx.getCacheOps().removeKeyAndData(pair);
        }

        String key = pair.getId();
        String type = pair.getType();

        String hashKey = hdataPrefix + "::" + type + ":" + key;
        String expKey = eventPrefix + "::" + type + ":" + key;

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

        stopWatch = context.startStopWatch("redis", "stringRedisTemplate.delete");
        AppCtx.getStringRedisTemplate().delete(hashKey);
        if (stopWatch != null) stopWatch.stopNow();

        LOGGER.trace("delete done");

    }

    @Override
    public void delete(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("findAndSave pairs(" + pairs.size() + "): " + pairs.printKey() +
                    "anyKey(" + anyKey.size() + "): " + anyKey.printTable());
        }

        if (enableDataCache) {
            AppCtx.getCacheOps().removeKeyAndData(pairs);
        }

        Set<String> hashKeys = new HashSet<>();
        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            String type = pair.getType();
            hashKeys.add(hdataPrefix + "::" + type + ":" + key);

            String expKey = eventPrefix + "::" + type + ":" + key;

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
