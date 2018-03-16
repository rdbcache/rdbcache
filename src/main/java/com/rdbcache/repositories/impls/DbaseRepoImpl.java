/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.*;
import com.rdbcache.models.*;
import com.rdbcache.queries.Parser;
import com.rdbcache.queries.QueryInfo;
import com.rdbcache.repositories.DbaseRepo;

import com.rdbcache.queries.Condition;
import com.rdbcache.queries.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class DbaseRepoImpl implements DbaseRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseRepoImpl.class);

    private boolean enableLocalCache = true;

    private boolean enableRedisCache = true;

    private Boolean enableDbFallback = PropCfg.getEnableDbFallback();

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        enableDbFallback = PropCfg.getEnableDbFallback();
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

    public Boolean getEnableDbFallback() {
        return enableDbFallback;
    }

    public void setEnableDbFallback(Boolean enableDbFallback) {
        this.enableDbFallback = enableDbFallback;
    }

    @Override
    public boolean find(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("find: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvFind(context, pairs, anyKey)) {
                return false;
            }
        } else {


            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.pepareSelect() || !query.executeSelect()) {
                if (enableDbFallback) {
                    if (!kvFind(context, pairs, anyKey)) {
                        return false;
                    } else {
                        String msg = "found (fallbacked to default table) " + anyKey.size() + " record(s)";
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    return false;
                }
            } else {

                LOGGER.debug("found (OK) " + anyKey.size() + " record(s) from " + table);
            }
        }

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

        return true;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("save: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        if (pairs.size() == 1) {
            return saveOne(context, pairs, anyKey);
        } else if (context.getAction().equals("save_post")) {
            boolean result = true;
            for (int i = 0; i < pairs.size(); i++) {
                KvPairs pairsNew = new KvPairs(pairs.get(i));
                KeyInfo keyInfo = anyKey.getAny(i);
                keyInfo.setParams(null);
                AnyKey anyKeyNew = new AnyKey(keyInfo);
                if (!saveOne(context, pairsNew, anyKeyNew)) {
                    result = false;
                }
            }
            return result;
        } else if (context.getAction().equals("pull_post")) {
            return update(context, pairs, anyKey);
        } else {
            throw new ServerErrorException(context.getAction() + " not supported case");
        }
    }

    @Override
    public boolean insert(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("insert: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvSave(context, pairs, anyKey)) {
                return false;
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.prepareInsert() || !query.executeInsert(enableLocalCache, enableRedisCache)) {
                if (enableDbFallback) {
                    if (!kvSave(context, pairs, anyKey)) {
                        return false;
                    } else {
                        String msg = "inserted (fallbacked to default table) " + anyKey.size() + " record(s)";
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    return false;
                }
            } else {

                LOGGER.debug("inserted (OK) " + anyKey.size() + " record(s) for " + table);
            }
        }

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

        return true;
    }

    @Override
    public boolean update(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("update: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvSave(context, pairs, anyKey)) {
                return false;
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.prepareUpdate() || !query.executeUpdate()) {
                if (enableDbFallback) {
                    if (!kvSave(context, pairs, anyKey)) {
                        return false;
                    } else {
                        String msg = "update (fallbacked to default table) " + anyKey.size() + " record(s)";
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    return false;
                }
            } else {

                LOGGER.debug("insert (OK) " + anyKey.size() + " record(s) from " + table);
            }
        }

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

        return true;
    }

    @Override
    public boolean delete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("delete: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        LOGGER.trace("delete: " + pairs.size() + " table: " + table);

        if (table == null) {
            if (!kvDelete(context, pairs, anyKey)) {
                return false;
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.prepareDelete() || !query.executeDelete()) {
                if (enableDbFallback) {
                    if (!kvDelete(context, pairs, anyKey)) {
                        return false;
                    } else {
                        String msg = "delete (fallbacked to default table) " + anyKey.size() + " record(s)";
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    return false;
                }
            } else {

                LOGGER.debug("deleted (OK) " + anyKey.size() + " record(s) from " + table);
            }
        }

        AppCtx.getKeyInfoRepo().delete(context, pairs,true);

        return true;
    }

    private boolean saveOne(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        KeyInfo keyInfo = anyKey.getAny();

        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("saveOne: " + key + " table: " +table);

        Map<String, Object> map = pair.getData();
        if (map == null || map.size() == 0) {
            String msg = "data is null or empty";
            LOGGER.error(msg);
            throw new ServerErrorException(context, msg);
        }

        // get it from database
        KvPairs dbPairs = new KvPairs(key);
        if (!find(context, dbPairs, anyKey)) {
            return insert(context, pairs, anyKey);
        }

        KvPair dbPair = dbPairs.getPair();

        String dbValue = dbPair.getValue();
        String value = pair.getValue();
        if (value != null && value.equals(dbValue)) {
            LOGGER.trace("identical as string");
            return true;
        }

        Map<String, Object> dbMap = dbPair.getData();
        if (dbMap != null && dbMap.size() > 0) {

            if (table != null) {

                String autoIncKey = AppCtx.getDbaseOps().getTableAutoIncColumn(context, table);
                if (autoIncKey != null && !map.containsKey(autoIncKey)) {
                    map.put(autoIncKey, dbMap.get(autoIncKey));
                    if (enableLocalCache) {
                        AppCtx.getLocalCache().updateData(key, map, keyInfo);
                    }
                    if (enableRedisCache) {
                        AppCtx.getRedisRepo().updateIfExists(context, new KvPairs(pair), new AnyKey(keyInfo));
                    }
                }

                Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
                if (!Utils.mapChangesAfterUpdate(map, dbMap, todoMap)) {
                    String msg = "switch to default table, unknown field found in input";
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                    setUseDefaultTable(keyInfo);
                    todoMap = map;
                }

                // identical map
                if (todoMap.size() == 0) {
                    LOGGER.trace("identical as map");
                    return true;
                }
                pair.setData(todoMap);

            } else if (Utils.isMapEquals(map, dbMap)) {

                LOGGER.trace("identical as map match");
                return true;
            }
        }

        if (table != null) {
            Parser.fetchStdClauseParams(context, keyInfo, dbMap, key);
        }

        return update(context, new KvPairs(pair), new AnyKey(keyInfo));
    }

    private boolean kvFind(Context context, KvPairs pairs, AnyKey anyKey) {

        Assert.isTrue(pairs.size() <= 1, "pairs.size() = " +
                pairs.size() + ", select only supports pairs size <= 1");

        KeyInfo keyInfo = anyKey.getAny();

        if (pairs.size() == 1) {

            KvPair pair = pairs.getPair();
            String key = pair.getId();

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
            KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "data"));
            if (stopWatch != null) stopWatch.stopNow();

            if (dbPair == null) {
                LOGGER.debug("not found from default table for " + key);
                return false;
            }

            setUseDefaultTable(keyInfo);
            pair.setData(dbPair.getData());

            LOGGER.debug("found from default table for " + key);

            return true;

        } else {

            QueryInfo queryInfo = keyInfo.getQueryInfo();
            if (queryInfo == null) {
                LOGGER.debug("no queryInfo");
                return false;
            }
            Map<String, Condition> conditions = queryInfo.getConditions();
            if (conditions == null ||
                    conditions.size() != 1 ||
                    !conditions.containsKey("key")) {
                LOGGER.error("no conditions or not key only");
                return false;
            }
            Condition condition = conditions.get("key");
            if (condition == null ||
                    condition.size() != 1 ||
                    !condition.containsKey("=")) {
                LOGGER.error("only = is supported");
                return false;
            }
            List<String> keys = condition.get("=");
            if (keys == null || keys.size() == 0) {
                LOGGER.error("condition is empty");
                return false;
            }
            List<KvIdType> idTypes = new ArrayList<KvIdType>();
            for (String key : keys) {
                KvIdType idType = new KvIdType(key, "data");
                idTypes.add(idType);
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findAll");
            Iterable<KvPair> dbPairs = AppCtx.getKvPairRepo().findAll(idTypes);
            if (stopWatch != null) stopWatch.stopNow();

            if (dbPairs == null && !dbPairs.iterator().hasNext()) {
                return false;
            }

            if (queryInfo != null) {
                keyInfo.setQueryInfo(null);
                Utils.getExcutorService().submit(() -> {
                    Thread.yield();
                    Parser.save(context, queryInfo);
                });
            }

            anyKey.clear();

            for (KvPair dbPair : dbPairs) {
                pairs.add(dbPair);
                KeyInfo keyInfoNew = new KeyInfo(keyInfo.getExpire());
                keyInfoNew.setIsNew(true);
                anyKey.add(keyInfo);
            }

            LOGGER.debug("found " + anyKey.size() + " from default");

            return true;
        }
    }

    private  boolean kvSave(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("kvSave: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        KvPairs pairsNew = new KvPairs();
        AnyKey anyKeyNew = new AnyKey();
        for (int i = 0; i < anyKey.size(); i++) {
            KeyInfo keyInfo = anyKey.getAny(i);
            if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
                setUseDefaultTable(keyInfo);
                pairsNew.add(pairs.get(i));
                anyKeyNew.add(keyInfo);
            }
        }

        if (anyKeyNew.size() == 0) {
            return true;
        }
        if (anyKeyNew.size() == 1) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().save(pairsNew.getPair());
                if (stopWatch != null) stopWatch.stopNow();

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

        } else {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().save(pairsNew);
                if (stopWatch != null) stopWatch.stopNow();

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }
        }

        return false;
    }


    private  boolean kvDelete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("kvDelete: " + pairs.getKeys().toString() + " keyInfo: " + anyKey.getAny().toString());

        KvPairs pairsNew = new KvPairs();
        AnyKey anyKeyNew = new AnyKey();
        for (int i = 0; i < anyKey.size(); i++) {
            KeyInfo keyInfo = anyKey.getAny(i);
            if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
                setUseDefaultTable(keyInfo);
                pairsNew.add(pairs.get(i));
                anyKeyNew.add(keyInfo);
            }
        }

        if (anyKeyNew.size() == 0) {
            return true;
        }
        if (anyKeyNew.size() == 1) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().delete(pairsNew.getPair());
                if (stopWatch != null) stopWatch.stopNow();

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

        } else {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().delete(pairsNew);
                if (stopWatch != null) stopWatch.stopNow();

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }
        }

        return false;
    }

    private void setUseDefaultTable(KeyInfo keyInfo) {
        keyInfo.setQueryKey(null);
        keyInfo.setTable(null);
        keyInfo.setClause(null);
        keyInfo.setParams(null);
        keyInfo.setQueryInfo(null);
    }
}