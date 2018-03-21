/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.configs.PropCfg;
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

    private boolean enableDataCache = true;

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
        if (PropCfg.getDataMaxCacheTLL() <= 0l) {
            enableDataCache = false;
        } else {
            enableDataCache = true;
        }
    }

    public boolean isEnableDataCache() {
        return enableDataCache;
    }

    public void setEnableDataCache(boolean enable) {
        this.enableDataCache = enable;
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

        LOGGER.trace("find: " + (pairs.size() > 0 ? pairs.getPair().getId() : "") + 
            (pairs.size() > 1 ? " ... " : " ") + anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvFind(context, pairs, anyKey)) {
                LOGGER.debug("find - kvFind failed: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
                return false;
            } else {
                LOGGER.debug("find - kvFind Ok: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifSelectOk() || !query.executeSelect()) {
                if (enableDbFallback) {
                    if (!kvFind(context, pairs, anyKey)) {
                        String msg = "find - not found fallbacked to default table: " +
                                pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ");
                        LOGGER.trace(msg);
                        return false;
                    } else {
                        String msg = "find - found fallbacked to default table Ok: " +
                                pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ");
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {

                    LOGGER.debug("find - not found: from " + table + " " +
                            pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
                    return false;
                }
            } else {

                LOGGER.debug("find - found Ok: from " + table + " " +
                        pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
            }
        }

        return true;
    }

    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("save: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        if (pairs.size() == 1) {

            if (saveOne(context, pairs, anyKey)) {
                LOGGER.debug("save - saveOne Ok: " + pairs.getPair().getId());
                return true;
            } else {
                LOGGER.debug("save - saveOne failed: " + pairs.getPair().getId());
                return false;
            }

        } else {

            boolean result = true;
            for (int i = 0; i < pairs.size(); i++) {

                KvPairs pairsNew = new KvPairs(pairs.get(i));
                KeyInfo keyInfo = anyKey.getAny(i);

                if (i >= anyKey.size()) {
                    keyInfo.clearParams();
                }
                AnyKey anyKeyNew = new AnyKey(keyInfo);

                if (!saveOne(context, pairsNew, anyKeyNew)) {
                    LOGGER.debug("save - saveOne failed: " + pairsNew.getPair().getId());
                    result = false;
                } else {
                    LOGGER.debug("save - saveOne Ok: " + pairsNew.getPair().getId());
                }
            }
            return result;
        }
    }

    @Override
    public boolean insert(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("insert: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvSave(context, pairs, anyKey)) {
                LOGGER.debug("insert kvSave failed: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
                return false;
            } else {
                LOGGER.debug("inserted kvSave Ok: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifInsertOk() || !query.executeInsert(enableDataCache, enableRedisCache)) {
                if (enableDbFallback) {
                    if (!kvSave(context, pairs, anyKey)) {
                        LOGGER.debug("insert failed - fallbacked to kvSave: " +
                                pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
                        return false;
                    } else {
                        String msg = "inserted Ok - fallbacked to kvSave: " +
                                pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ");
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    LOGGER.debug("insert failed: " + pairs.getPair().getId() +
                            (pairs.size() > 1 ? " ... " : " "));
                    return false;
                }
            } else {
                LOGGER.debug("insert Ok: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " ") + " for " + table);
            }
        }

        return true;
    }

    @Override
    public boolean update(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("update: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        if (table == null) {
            if (!kvSave(context, pairs, anyKey)) {
                LOGGER.debug("update kvSave failed: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
                return false;
            } else {
                LOGGER.debug("updated kvSave Ok: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifUpdateOk() || !query.executeUpdate()) {
                if (enableDbFallback) {
                    if (!kvSave(context, pairs, anyKey)) {
                        LOGGER.debug("update failed - fallback to kvSave: " + pairs.getPair().getId() +
                                (pairs.size() > 1 ? " ... " : " "));
                        return false;
                    } else {
                        String msg = "update Ok -  fallbacked to kvSave: " + pairs.getPair().getId() +
                                (pairs.size() > 1 ? " ... " : " ");
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    LOGGER.debug("update failed - " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " "));
                    return false;
                }
            } else {
                LOGGER.debug("updated Ok: " + anyKey.size() + " record(s) from " + table);
            }
        }

        return true;
    }

    @Override
    public boolean delete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("delete: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        String table = anyKey.getKey().getTable();

        LOGGER.trace("delete: " + pairs.size() + " table: " + table);

        if (table == null) {
            if (!kvDelete(context, pairs, anyKey)) {
                LOGGER.debug("delete kvDelete failed: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
                return false;
            } else {
                LOGGER.debug("deleted kvDelete Ok: " + pairs.getPair().getId() +
                        (pairs.size() > 1 ? " ... " : " "));
            }
        } else {

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifDeleteOk() || !query.executeDelete()) {
                if (enableDbFallback) {
                    if (!kvDelete(context, pairs, anyKey)) {
                        LOGGER.debug("delete failed - fallback to kvDelete: " + pairs.getPair().getId() +
                                (pairs.size() > 1 ? " ... " : " "));
                        return false;
                    } else {
                        String msg = "delete Ok - fallbacked to kvDelete" + pairs.getPair().getId() +
                                (pairs.size() > 1 ? " ... " : " ");
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    return false;
                }
            } else {
                LOGGER.debug("deleted Ok: " + anyKey.size() + " record(s) from " + table);
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

        Map<String, Object> map = pair.getData();

        if (pair.isUuid()) {
            return insert(context, pairs, anyKey);
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
                    if (enableDataCache) {
                        AppCtx.getLocalCache().updateData(pair);
                    }
                    if (enableRedisCache) {
                        AppCtx.getRedisRepo().updateIfExist(context, new KvPairs(pair), new AnyKey(keyInfo));
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
            Parser.prepareStandardClauseParams(context, dbPair, keyInfo);
        }

        return update(context, new KvPairs(pair), new AnyKey(keyInfo));
    }

    private boolean kvFind(Context context, KvPairs pairs, AnyKey anyKey) {

        Assert.isTrue(pairs.size() <= 1, "pairs.size() = " +
                pairs.size() + ", select only supports pairs size <= 1");

        LOGGER.trace("kvFind: " + (pairs.size() == 1 ? pairs.getPair().getId() : "") + anyKey.getAny().toString());

        KeyInfo keyInfo = anyKey.getAny();

        if (pairs.size() == 1) {

            KvPair pair = pairs.getPair();
            String key = pair.getId();

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
            KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "data"));
            if (stopWatch != null) stopWatch.stopNow();

            if (dbPair == null) {
                LOGGER.trace("kvFind: not found from default table for " + key);
                return false;
            }

            setUseDefaultTable(keyInfo);
            pair.setData(dbPair.getData());

            LOGGER.trace("kvFind(1) Ok - found from default table for " + key);

            return true;

        } else {

            QueryInfo queryInfo = keyInfo.getQueryInfo();
            if (queryInfo == null) {               // no queryKey means no query
                return false;
            }
            Map<String, Condition> conditions = queryInfo.getConditions();
            if (conditions == null ||              // must have condtion
                conditions.size() != 1 ||          // only 1 condition allowed
                !conditions.containsKey("key")) {  // only condition with key allowed
                return false;
            }
            Condition condition = conditions.get("key");
            if (condition == null ||               // must have condition
                condition.size() != 1 ||           // only 1 condition
                !condition.containsKey("=")) {     // only = allowed
                return false;
            }
            List<String> keys = condition.get("=");
            if (keys == null || keys.size() == 0) {
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
                LOGGER.trace("kvFind(" + anyKey.size() + ") failed to find from default table");
                return false;
            }

            if (queryInfo != null) {
                keyInfo.setQueryInfo(null);
                Utils.getExcutorService().submit(() -> {
                    Thread.yield();
                    Parser.saveQuery(context, queryInfo);
                });
            }

            anyKey.clear();
            int i = 0;
            for (KvPair dbPair : dbPairs) {

                pairs.add(dbPair);
                anyKey.getAny(i++);
            }

            LOGGER.trace("kvFind(" + anyKey.size() + ") Ok - found from default table");

            return true;
        }
    }

    private  boolean kvSave(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("kvSave: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        if (anyKey.size() == 0) {
            LOGGER.trace("kvSave(0) Ok - nothing to save");
            return true;
        }
        if (anyKey.size() == 1) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().save(pairs.getPair());
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.trace("kvSave(1) Ok");

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
                AppCtx.getKvPairRepo().save(pairs);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.trace("kvSave(" + pairs.size() + ") Ok");

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }
        }

        LOGGER.trace("kvSave(" + pairs.size() + ") failed");

        return false;
    }


    private  boolean kvDelete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("kvDelete: " + pairs.getPair().getId() + (pairs.size() > 1 ? " ... " : " ") +
                anyKey.getAny().toString());

        if (anyKey.size() == 0) {
            LOGGER.trace("kvDelete(0) Ok - nothing to delete");
            return true;
        }
        if (anyKey.size() == 1) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            try {
                AppCtx.getKvPairRepo().delete(pairs.getPair());
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.trace("kvDelete(1) Ok");

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
                AppCtx.getKvPairRepo().delete(pairs);
                if (stopWatch != null) stopWatch.stopNow();

                LOGGER.trace("kvDelete(" + pairs.size() + ") Ok");

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }
        }

        LOGGER.trace("kvDelete(" + pairs.size() + ") failed");

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