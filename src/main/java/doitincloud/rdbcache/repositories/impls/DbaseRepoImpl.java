/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.repositories.impls;

import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.commons.helpers.*;
import doitincloud.rdbcache.models.*;
import doitincloud.rdbcache.queries.Parser;
import doitincloud.rdbcache.queries.QueryInfo;
import doitincloud.rdbcache.repositories.DbaseRepo;

import doitincloud.rdbcache.queries.Condition;
import doitincloud.rdbcache.queries.Query;
import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.DbUtils;
import doitincloud.rdbcache.supports.KvPairs;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;

@Repository
public class DbaseRepoImpl implements DbaseRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseRepoImpl.class);

    private boolean enableDataCache = true;

    private boolean enableRedisCache = true;

    private Boolean enableDbFallback = PropCfg.getEnableDbFallback();

    @PostConstruct
    public void init() {
        //System.out.println("*** init DbaseRepoImpl");
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

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
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
    public boolean find(final Context context, final KvPair pair, final KeyInfo keyInfo) {
        KvPairs pairs = new KvPairs(pair);
        AnyKey anyKey = new AnyKey(keyInfo);
        return find(context, pairs, anyKey);
    }

    @Override
    public boolean find(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("find: " + pairs.printKey() + anyKey.print());
        }

        String table = anyKey.getKeyInfo().getTable();

        if (table == null) {
            if (!kvFind(context, pairs, anyKey)) {
                LOGGER.debug("find - kvFind failed: " + pairs.printKey());
                return false;
            } else {
                LOGGER.debug("find - kvFind Ok: " + pairs.printKey());
            }
            AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
            return true;

        } else {

            JdbcTemplate jdbcTemplate = AppCtx.getDbaseOps().getJdbcTemplate(context, table);
            boolean allOk = true;

            if (anyKey.size() == 1) {

                Query query = new Query(context, jdbcTemplate, pairs, anyKey);

                if (!query.ifSelectOk() || !query.executeSelect()) {
                    if (enableDbFallback) {
                        if (!kvFind(context, pairs, anyKey)) {
                            String msg = "find - not found fallbacked to default table: " + pairs.printKey();
                            LOGGER.trace(msg);
                            allOk = false;
                        } else {
                            String msg = "find - found fallbacked to default table Ok: " + pairs.printKey();
                            context.logTraceMessage(msg);
                            LOGGER.warn(msg);
                        }
                    } else {
                        LOGGER.debug("find - not found: from " + table + " " + pairs.printKey());
                        allOk = false;
                    }
                } else {
                    LOGGER.debug("find - found Ok: from " + table + " " + pairs.printKey());
                    AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
                }

            } else {

                for (int i = 0; i < anyKey.size(); i++) {

                    KeyInfo keyInfo = anyKey.get(i);

                    if (keyInfo.getQuery() != null) {
                        throw new ServerErrorException("query for muliple keyInfos is not supported");
                    }

                    table = keyInfo.getTable();
                    AnyKey newAnyKey = new AnyKey(keyInfo);

                    KvPair pair = pairs.getAny(i);
                    KvPairs newPairs = new KvPairs(pair);

                    Query query = new Query(context, jdbcTemplate, newPairs, newAnyKey);

                    if (!query.ifSelectOk() || !query.executeSelect()) {
                        if (enableDbFallback) {
                            if (!kvFind(context, newPairs, newAnyKey)) {
                                String msg = "find - not found fallbacked to default table: " + newPairs.printKey();
                                LOGGER.trace(msg);
                                allOk = false;
                            } else {
                                String msg = "find - found fallbacked to default table Ok: " + newPairs.printKey();
                                context.logTraceMessage(msg);
                                LOGGER.warn(msg);
                            }
                        } else {
                            LOGGER.debug("find - not found: from " + table + " " + newPairs.printKey());
                            allOk = false;
                        }
                    } else {
                        LOGGER.debug("find - found Ok: from " + table + " " + pairs.printKey());
                        AppCtx.getKeyInfoRepo().save(context, newPairs, newAnyKey);
                    }
                }
            }

            return allOk;
        }
    }

    @Override
    public boolean save(final Context context, final KvPair pair, final KeyInfo keyInfo) {
        if (saveOne(context, pair, keyInfo)) {
            LOGGER.debug("save - saveOne Ok: " + pair.printKey());
            AppCtx.getKeyInfoRepo().save(context, pair, keyInfo);
            return true;
        } else {
            LOGGER.debug("save - saveOne failed: " + pair.printKey());
            return false;
        }
    }

    @Override
    public boolean save(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("save: " + pairs.printKey() + anyKey.print());
        }

        if (pairs.size() == 1) {

            if (saveOne(context, pairs.getPair(), anyKey.getKeyInfo())) {
                LOGGER.debug("save - saveOne Ok: " + pairs.getPair().printKey());
                AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
                return true;
            } else {
                LOGGER.debug("save - saveOne failed: " + pairs.getPair().printKey());
                return false;
            }

        } else {

            boolean result = true;
            for (int i = 0; i < pairs.size(); i++) {

                KvPair pair = pairs.get(i);
                KeyInfo keyInfo = anyKey.getAny(i);

                if (!saveOne(context, pair, keyInfo)) {
                    LOGGER.debug("save - saveOne failed: " + pair.getId());
                    result = false;
                } else {
                    AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));
                    LOGGER.debug("save - saveOne Ok: " + pair.getId());
                }
            }
            return result;
        }
    }

    @Override
    public boolean insert(final Context context, final KvPair pair, final KeyInfo keyInfo) {
        KvPairs pairs = new KvPairs(pair);
        AnyKey anyKey = new AnyKey(keyInfo);
        return insert(context, pairs, anyKey);
    }

    @Override
    public boolean insert(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("insert: " + pairs.printKey() + anyKey.print());
        }

        String table = anyKey.getKeyInfo().getTable();

        if (table == null) {
            if (!kvSave(context, pairs, anyKey)) {
                LOGGER.debug("insert kvSave failed: " + pairs.printKey());
                return false;
            } else {
                AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
                LOGGER.debug("inserted kvSave Ok: " + pairs.printKey());
            }
        } else {

            JdbcTemplate jdbcTemplate = AppCtx.getDbaseOps().getJdbcTemplate(context, table);

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifInsertOk() || !query.executeInsert(enableDataCache, enableRedisCache)) {
                if (enableDbFallback) {
                    if (!kvSave(context, pairs, anyKey)) {
                        LOGGER.debug("insert failed - fallbacked to kvSave: " +
                                pairs.printKey());
                        return false;
                    } else {
                        String msg = "inserted Ok - fallbacked to kvSave: " +
                                pairs.printKey();
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    LOGGER.debug("insert failed: " + pairs.printKey());
                    return false;
                }
            } else {
                AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
                LOGGER.debug("insert Ok: " + pairs.getPair().getId() + pairs.printKey() + " for " + table);
            }
        }

        return true;
    }

    @Override
    public boolean update(final Context context, final KvPair pair, final KeyInfo keyInfo) {
        KvPairs pairs = new KvPairs(pair);
        AnyKey anyKey = new AnyKey(keyInfo);
        return update(context, pairs, anyKey);
    }

    @Override
    public boolean update(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("update: " + pairs.printKey() + anyKey.print());
        }

        String table = anyKey.getKeyInfo().getTable();

        if (table == null) {
            if (!kvUpdate(context, pairs, anyKey)) {
                LOGGER.debug("update kvSave failed: " + pairs.printKey());
                return false;
            } else {
                LOGGER.debug("updated kvSave Ok: " + pairs.printKey());
            }
        } else {

            JdbcTemplate jdbcTemplate = AppCtx.getDbaseOps().getJdbcTemplate(context, table);

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifUpdateOk() || !query.executeUpdate()) {
                if (enableDbFallback) {
                    if (!kvUpdate(context, pairs, anyKey)) {
                        LOGGER.debug("update failed - fallback to kvSave: " + pairs.printKey());
                        return false;
                    } else {
                        String msg = "update Ok -  fallbacked to kvSave: " + pairs.printKey();
                        context.logTraceMessage(msg);
                        LOGGER.warn(msg);
                    }
                } else {
                    LOGGER.debug("update failed - " + pairs.printKey());
                    return false;
                }
            } else {
                AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);
                LOGGER.debug("updated Ok: " + anyKey.size() + " record(s) from " + table);
            }
        }

        return true;
    }

    @Override
    public boolean delete(final Context context, final KvPair pair, final KeyInfo keyInfo) {
        KvPairs pairs = new KvPairs(pair);
        AnyKey anyKey = new AnyKey(keyInfo);
        return delete(context, pairs, anyKey);
    }

    @Override
    public boolean delete(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("delete: " + pairs.printKey() + anyKey.print());
        }

        String table = anyKey.getKeyInfo().getTable();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("delete: " + pairs.size() + " table: " + table);
        }

        if (table == null) {
            if (!kvDelete(context, pairs, anyKey)) {
                LOGGER.debug("delete kvDelete failed: " + pairs.printKey());
                return false;
            } else {
                LOGGER.debug("deleted kvDelete Ok: " + pairs.printKey());
            }
        } else {

            JdbcTemplate jdbcTemplate = AppCtx.getDbaseOps().getJdbcTemplate(context, table);

            Query query = new Query(context, jdbcTemplate, pairs, anyKey);

            if (!query.ifDeleteOk() || !query.executeDelete()) {
                if (enableDbFallback) {
                    if (!kvDelete(context, pairs, anyKey)) {
                        LOGGER.debug("delete failed - fallback to kvDelete: " + pairs.printKey());
                        return false;
                    } else {
                        String msg = "delete Ok - fallbacked to kvDelete " + pairs.printKey();
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

        return true;
    }

    private boolean saveOne(final Context context, final KvPair pair, final KeyInfo keyInfo) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("saveOne: " + pair.getId() + " " + keyInfo.toString());
        }

        String key = pair.getId();
        String table = keyInfo.getTable();

        Map<String, Object> map = pair.getData();

        if (pair.isNewUuid() && keyInfo.getQuery() == null && keyInfo.getParams() == null) {
            return insert(context, new KvPairs(pair), new AnyKey(keyInfo));
        }

        // get it from database
        KvPairs dbPairs = new KvPairs(pair.getIdType());
        if (!find(context, dbPairs, new AnyKey(keyInfo))) {
            return insert(context, new KvPairs(pair), new AnyKey(keyInfo));
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
                        AppCtx.getCacheOps().updateData(pair);
                    }
                    if (enableRedisCache) {
                        if (AppCtx.getRedisRepo().ifExist(context, new KvPairs(pair), new AnyKey(keyInfo))) {
                            AppCtx.getRedisRepo().update(context, new KvPairs(pair), new AnyKey(keyInfo));
                        }
                    }
                }

                Map<String, Object> indexes = AppCtx.getDbaseOps().getTableIndexes(context, table);
                List<String> primaryIndex = (List<String>) indexes.get("PRIMARY");

                Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
                if (!DbUtils.mapChangesAfterUpdate(map, dbMap, todoMap, primaryIndex)) {
                    if (enableDbFallback) {
                        String msg = "switch to default table, unknown field found in input: " + Utils.toJson(todoMap);
                        LOGGER.error(msg);
                        context.logTraceMessage(msg);
                        setUseDefaultTable(keyInfo);
                        todoMap = map;
                    } else {
                        //System.out.println("map: " + Utils.toPrettyJson(map));
                        //System.out.println("dbMap: " + Utils.toPrettyJson(dbMap));
                        String msg = "unknown field found in input: " + Utils.toJson(todoMap);
                        LOGGER.error(msg);
                        context.logTraceMessage(msg);
                        if (context.isSync()) {
                            throw new ServerErrorException(context, msg);
                        }
                        return false;
                    }
                }

                // identical map
                if (todoMap.size() == 0) {
                    LOGGER.trace("identical as map");
                    return true;
                }
                pair.setData(todoMap);

            } else if (DbUtils.isMapEquals(map, dbMap)) {

                LOGGER.trace("identical as map match");
                return true;
            }
        }

        if (table != null) {
            Parser.prepareStandardClauseParams(context, dbPair, keyInfo);
        }

        return update(context, new KvPairs(pair), new AnyKey(keyInfo));
    }

    private boolean kvFind(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("kvFind: " + pairs.printKey() + anyKey.print());
        }

        KeyInfo keyInfo = anyKey.getKeyInfo();

        if (pairs.size() == 1) {

            KvPair pair = pairs.getPair();
            String key = pair.getId();

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
            KvPair dbPair = AppCtx.getKvPairRepo().findById(new KvIdType(key, "data"));
            if (stopWatch != null) stopWatch.stopNow();

            if (dbPair == null) {
                if (LOGGER.isTraceEnabled()) LOGGER.trace("kvFind: not found from default table for " + key);
                return false;
            }
            setUseDefaultTable(keyInfo);
            pair.setData(dbPair.getData());

            if (LOGGER.isTraceEnabled()) LOGGER.trace("kvFind(1) Ok - found from default table for " + key);

            return true;

        } else {

            QueryInfo queryInfo = keyInfo.getQuery();
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
            Iterable<KvPair> dbPairs = AppCtx.getKvPairRepo().findAllById(idTypes);
            if (stopWatch != null) stopWatch.stopNow();

            if (dbPairs == null && !dbPairs.iterator().hasNext()) {
                if (LOGGER.isTraceEnabled()) LOGGER.trace("kvFind(" + anyKey.size() + ") failed to find from default table");
                return false;
            }

            if (queryInfo != null) {
                keyInfo.setQuery(null);
                Utils.getExcutorService().submit(() -> {
                    Thread.yield();
                    AppCtx.getDbaseOps().saveQuery(context, queryInfo);
                });
            }

            anyKey.clear();
            int i = 0;
            for (KvPair dbPair: dbPairs) {
                pairs.add(dbPair);
                anyKey.getAny(i++);
            }

            if (LOGGER.isTraceEnabled()) LOGGER.trace("kvFind(" + anyKey.size() + ") Ok - found from default table");

            return true;
        }
    }

    private boolean kvUpdate(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        KvPairs pairsClone = pairs.clone();
        kvFind(context, pairsClone, anyKey);
        for (int i = 0; i < pairsClone.size(); i++) {
            Map<String, Object> data = pairsClone.get(i).getData();
            Map<String, Object> update = pairs.get(i).getData();
            for (Map.Entry<String, Object> entry: update.entrySet()) {
                data.put(entry.getKey(), entry.getValue());
            }
        }
        return kvSave(context, pairsClone, anyKey);
    }

    private boolean kvSave(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("kvSave: " + pairs.printKey() + anyKey.print());
        }

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
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }

        } else {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.saveAll");
            try {
                AppCtx.getKvPairRepo().saveAll(pairs);
                if (stopWatch != null) stopWatch.stopNow();

                if (LOGGER.isTraceEnabled()) LOGGER.trace("kvSave(" + pairs.size() + ") Ok");

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("kvSave(" + pairs.size() + ") failed");

        return false;
    }


    private boolean kvDelete(final Context context, final KvPairs pairs, final AnyKey anyKey) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("kvDelete: " + pairs.printKey() + anyKey.print());
        }

        if (anyKey.size() == 0) {
            LOGGER.trace("kvDelete(0) Ok - nothing to delete");
            return true;
        }
        if (anyKey.size() == 1) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.delete");
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
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }

        } else {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.deleteAll");
            try {
                AppCtx.getKvPairRepo().deleteAll(pairs);
                if (stopWatch != null) stopWatch.stopNow();

                if (LOGGER.isTraceEnabled()) LOGGER.trace("kvDelete(" + pairs.size() + ") Ok");

                return true;

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
                if (context.isSync()) {
                    throw new ServerErrorException(context, msg);
                }
            }
        }

        if (LOGGER.isTraceEnabled()) LOGGER.trace("kvDelete(" + pairs.size() + ") failed");

        return false;
    }

    private void setUseDefaultTable(KeyInfo keyInfo) {
        keyInfo.setQueryKey(null);
        keyInfo.setTable(null);
        keyInfo.setClause(null);
        keyInfo.setParams(null);
        keyInfo.setQuery(null);
    }
}