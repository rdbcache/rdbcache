/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.impls;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.*;
import com.rdbcache.models.*;
import com.rdbcache.queries.QueryInfo;
import com.rdbcache.repositories.DbaseRepo;

import com.rdbcache.queries.Condition;
import com.rdbcache.queries.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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

        LOGGER.trace("find pairs(" + pairs.size() + ") anyKey(" + anyKey.size() + "): " + anyKey.getAny().toString());

        if (anyKey.size() > 1) {
            throw new ServerErrorException("unsupported condtion");
        }

        KvPair pair = pairs.getPair();
        KeyInfo keyInfo = anyKey.getAny();
        String table = keyInfo.getTable();

        if (table != null && Query.readyForQuery(context, pair, keyInfo)) {

            Map<String, Object> columns = Query.fetchColumns(context, keyInfo);
            List<String> indexes = Query.fetchIndexes(context, keyInfo);

            String clause = keyInfo.getClause();
            List<Object> params = keyInfo.getParams();

            String sql = "select * from " + table;

            if (clause != null && clause.length() > 0) {
                sql += " where " + clause;
            }

            int limit = getLimt(context, pairs, keyInfo);
            if (limit > 0) {
                sql += " limit " + limit;
            }

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            try {
                List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
                if (stopWatch != null) stopWatch.stopNow();

                if (list.size() > 0) {

                    LOGGER.debug("found " + list.size() + " record(s) from " + table);

                    AnyKey anyKeyNew = new AnyKey();

                    for (int i = 0; i < list.size(); i++) {

                        Map<String, Object> map = convertDbMap(columns, list.get(i));

                        KvPair pairNew = null;
                        if (i == pairs.size()) {
                            pairNew = new KvPair(Utils.generateId(), map);
                            pairs.add(pairNew);
                        } else {
                            pairNew = pairs.get(i);
                            pairNew.setData(map);
                        }

                        KeyInfo keyInfoNew = new KeyInfo(keyInfo.getExpire(), table);

                        keyInfoNew.setIndexes(indexes);
                        keyInfoNew.setColumns(columns);
                        Query.fetchClauseParams(context, keyInfoNew, map, pairNew.getId());
                        keyInfoNew.setIsNew(true);

                        anyKeyNew.add(keyInfoNew);
                    }

                    AppCtx.getKeyInfoRepo().save(context, pairs, anyKeyNew);

                    keyInfo.setIsNew(false);
                    final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
                    if (finalQueryInfo != null) {
                        keyInfo.setQueryInfo(null);
                        Utils.getExcutorService().submit(() -> {
                            Thread.yield();
                            Query.save(context, finalQueryInfo);
                        });
                    }
                    return true;

                } else {

                    LOGGER.debug("not found any record from " + table);
                }

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

            String msg = "find fallbacked to default table for " + pair.getId();
            LOGGER.warn(msg);
            if (enableDbFallback) {
                context.logTraceMessage(msg);
            } else {
                throw new BadRequestException(context, "failed to update database");
            }
        }

        // otherwise find it from default table
        //
        if (pairs.size() == 1) {
            return kvTableFindOne(context, pairs, anyKey);
        } else {
            return kvTableFindAll(context, pairs, anyKey);
        }
    }

    private boolean kvTableFindOne(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "data"));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null) {
            LOGGER.debug("not found from default table for " + key);
            return false;
        }

        KeyInfo keyInfo = anyKey.getAny();

        setUseDefaultTable(keyInfo);

        pair.setData(dbPair.getData());

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

        LOGGER.debug("found from default table for " + key);

        return true;

    }

    private boolean kvTableFindAll(Context context, KvPairs pairs, AnyKey anyKey) {

        KeyInfo keyInfo = anyKey.getAny();
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

        final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
        if (finalQueryInfo != null) {
            keyInfo.setQueryInfo(null);
            Utils.getExcutorService().submit(() -> {
                Thread.yield();
                Query.save(context, finalQueryInfo);
            });
        }

        AnyKey anyKeyNew = new AnyKey();
        for (KvPair dbPair : dbPairs) {
            pairs.add(dbPair);
            KeyInfo keyInfoNew = new KeyInfo(keyInfo.getExpire());
            keyInfoNew.setIsNew(true);
            anyKeyNew.add(keyInfoNew);
        }

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKeyNew);

        LOGGER.debug("found " + pairs.size() + " from default table");

        return true;
    }

    private boolean findOne(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();
        KeyInfo keyInfo = anyKey.getAny();
        String table = keyInfo.getTable();

        LOGGER.trace("findOne: " + key + " table: " + table);
        LOGGER.trace("keyInfo: " + keyInfo.toString());

        if (table != null && Query.readyForQuery(context, pair, keyInfo)) {

            System.out.println("*** here");
            String clause = keyInfo.getClause();
            List<Object> params = keyInfo.getParams();
            Map<String, Object> columns = Query.fetchColumns(context, keyInfo);

            String sql = "select * from " + table + " where " + clause + " limit 1";

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            try {
                List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
                if (stopWatch != null) stopWatch.stopNow();

                if (list.size() > 0) {

                    LOGGER.debug("found from " + table + " for " + key);

                    Map<String, Object> map = convertDbMap(columns, list.get(0));
                    pair.setData(map);

                    Query.fetchClauseParams(context, keyInfo, map, key);

                    AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));

                    final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
                    if (finalQueryInfo != null) {
                        keyInfo.setQueryInfo(null);
                        Utils.getExcutorService().submit(() -> {
                            Thread.yield();
                            Query.save(context, finalQueryInfo);
                        });
                    }

                    return true;

                } else {
                    LOGGER.debug("not found from " + table + " for " + key);
                }
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                if (enableDbFallback) {
                    context.logTraceMessage(msg);
                } else {
                    throw new BadRequestException(context, msg);
                }
            }
        }

        // otherwise find it from default table
        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key, "data"));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null) {
            LOGGER.debug("not found from default table for " + key);
            return false;
        }

        setUseDefaultTable(keyInfo);
        pair.setData(dbPair.getData());

        AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));

        LOGGER.debug("found from default table for " + key);

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
        if (!findOne(context, dbPairs, anyKey)) {
            return insertOne(context, pair, keyInfo);
        }

        String dbValue = dbPairs.getPair().getValue();
        String value = pair.getValue();
        if (value != null && value.equals(dbValue)) {
            LOGGER.trace("identical as string");
            return true;
        }

        Map<String, Object> dbMap = dbPairs.getPair().getData();
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

        if (table != null && !Query.hasStdClause(context, keyInfo)) {
            Query.fetchClauseParams(context, keyInfo, dbMap, key);
        }

        return update(context, new KvPairs(pair), new AnyKey(keyInfo));
    }


    @Override
    public boolean save(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("save pairs(" + pairs.size() + ") anyKey(" + anyKey.size() + "): " + anyKey.getAny().toString());

        if (pairs.size() == 1) {
            return saveOne(context, pairs, anyKey);
        }

        KeyInfo keyInfo = anyKey.getKey();
        //KvPair pair = pairs.getPair();

        String table = keyInfo.getTable();

        List<String> indexes = Query.fetchIndexes(context, keyInfo);

        final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
        if (finalQueryInfo != null) {
            keyInfo.setQueryInfo(null);
            Utils.getExcutorService().submit(() -> {
                Thread.yield();
                Query.save(context, finalQueryInfo);
            });
        }

        for (KvPair pair2: pairs) {

            String key = pair2.getId();
            KeyInfo keyInfoPer = keyInfo.clone();

            Map<String, Object> map = pair2.getData();
            if (map == null || map.size() == 0) {
                String msg = "data is null or empty";
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                continue;
            }

            // get it from database
            KvPairs dbPairs = new KvPairs(key);
            if (!find(context, dbPairs, new AnyKey(keyInfoPer))) {
                insert(context, new KvPairs(pair2), new AnyKey(keyInfoPer));
                continue;
            }

            KvPair dbPair = dbPairs.getPair();
            String dbValue = dbPair.getValue();
            String value = pair2.getValue();
            if (value != null && value.equals(dbValue)) {
                LOGGER.trace("identical as string");
                continue;
            }

            Map<String, Object> dbMap = dbPair.getData();

            if (dbMap != null && dbMap.size() > 0) {

                if (table != null) {

                    String autoIncKey = AppCtx.getDbaseOps().getTableAutoIncColumn(context, table);
                    if (autoIncKey != null && !map.containsKey(autoIncKey)) {
                        map.put(autoIncKey, dbMap.get(autoIncKey));
                        if (enableLocalCache) {
                            AppCtx.getLocalCache().updateData(key, map, keyInfoPer);
                        }
                        if (enableRedisCache) {
                            AppCtx.getRedisRepo().updateIfExists(context, new KvPairs(pair2), new AnyKey(keyInfoPer));
                        }
                    }

                    Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
                    if (!Utils.mapChangesAfterUpdate(map, dbMap, todoMap)) {
                        String msg = "unknown field found in input";
                        LOGGER.error(msg);
                        throw new BadRequestException(context, msg);
                    }

                    // identical map
                    if (todoMap.size() == 0) {
                        LOGGER.trace("identical as map");
                        continue;
                    }
                    pair2.setData(todoMap);
                } else if (Utils.isMapEquals(map, dbMap)) {

                    LOGGER.trace("identical as map match");
                    continue;
                }
            }

            keyInfoPer.setIndexes(indexes);

            if (table != null && !Query.hasStdClause(context, keyInfoPer)) {
                Query.fetchClauseParams(context, keyInfoPer, dbMap, key);
            }

            update(context, new KvPairs(pair2), new AnyKey(keyInfoPer));
        }
        return true;
    }

    private boolean insertOne(Context context, KvPair pair, KeyInfo keyInfo) {

        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("insertOne: " + key + " table: " + table);

        Map<String, Object> map = pair.getData();
        if (map == null || map.size() == 0) {
            String msg = "data is null or empty";
            LOGGER.error(msg);
            throw new ServerErrorException(context, msg);
        }

        if (table != null && Query.readyForInsert(context, pair, keyInfo)) {

            ArrayList<Object> queryParams = new ArrayList<Object>();
            String fields = "", values = "";

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                queryParams.add(entry.getValue());
                if (fields.length() != 0) {
                    fields += ", ";
                    values += ", ";
                }
                fields += entry.getKey();
                values += "?";
            }

            String sql = "insert into " + table + " (" + fields + ") values(" + values + ")";
            KeyHolder keyHolder = new GeneratedKeyHolder();
            int rowCount = 0;

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
            try {
                rowCount = jdbcTemplate.update(new PreparedStatementCreator() {

                    @Override
                    public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                        PreparedStatement ps;
                        ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                        int i = 1;
                        for(Object param: queryParams) {
                            ps.setObject(i++, param);
                        }
                        return ps;
                    }
                }, keyHolder);
                if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                if (enableDbFallback) {
                    context.logTraceMessage(msg);
                } else {
                    throw new BadRequestException(context, msg);
                }
            }

            if (rowCount > 0) {

                String autoIncKey = AppCtx.getDbaseOps().getTableAutoIncColumn(context, table);
                if (autoIncKey != null && keyHolder.getKey() == null) {
                    LOGGER.error("failed to get auto increment id from query");
                }
                if (autoIncKey != null && keyHolder.getKey() != null) {
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(autoIncKey, keyValue);
                    if (enableLocalCache) {
                        AppCtx.getLocalCache().putData(key, map, keyInfo);
                    }
                    if (enableRedisCache) {
                        AppCtx.getRedisRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));
                    }
                }
                Query.fetchClauseParams(context, keyInfo, map, key);
                AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));

                keyInfo.setQueryInfo(null);
                LOGGER.debug("insert ok: " + table + " for " + pair.getId());
                return true;
            }

            if (enableDbFallback) {
                String msg = "save fallbacked to default table for " + pair.getId();
                LOGGER.warn(msg);
                context.logTraceMessage(msg);
            } else {
                throw new BadRequestException(context, "failed to insert into database");
            }
        }

        // otherwise save it to default table
        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        AppCtx.getKvPairRepo().save(pair);
        if (stopWatch != null) stopWatch.stopNow();

        setUseDefaultTable(keyInfo);

        AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));

        return true;
    }

    @Override
    public boolean insert(Context context, KvPairs pairs, AnyKey anyKey) {

        KeyInfo keyInfo = anyKey.getKey();

        if (pairs.size() == 1) {
            return insertOne(context, pairs.getPair(), keyInfo);
        }

        String table = keyInfo.getTable();

        LOGGER.trace("insert: #pairs = " + pairs.size() + " table: " + table);

        if (table == null) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.InsertAll");
            try {
                AppCtx.getKvPairRepo().save(pairs);
                if (stopWatch != null) stopWatch.stopNow();

                List<KeyInfo> keyInfos = new ArrayList<>();
                for (KvPair pair: pairs) {
                    keyInfos.add(new KeyInfo(keyInfo.getExpire()));
                }
                AppCtx.getKeyInfoRepo().save(context, pairs, new AnyKey(keyInfos));

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();

                return false;
            }

            return true;
        }

        if (!Query.readyForInsert(context, pairs.getPair(), keyInfo)) {
            return false;
        }

        List<String> indexes = Query.fetchIndexes(context, keyInfo);

        List<KeyInfo> keyInfos = new ArrayList<KeyInfo>();

        String autoIncKey = AppCtx.getDbaseOps().getTableAutoIncColumn(context, table);

        for (KvPair pair: pairs) {

            String key = pair.getId();
            Map<String, Object> map = pair.getData();
            System.out.println("map: "+Utils.toJson(map));

            ArrayList<Object> queryParams = new ArrayList<Object>();
            String fields = "", values = "";

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                queryParams.add(entry.getValue());
                if (fields.length() != 0) {
                    fields += ", ";
                    values += ", ";
                }
                fields += entry.getKey();
                values += "?";
            }

            String sql = "insert into " + table + " (" + fields + ") values(" + values + ")";
            System.out.println("sql: " + sql);
            KeyHolder keyHolder = new GeneratedKeyHolder();
            int rowCount = 0;

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
            try {
                rowCount = jdbcTemplate.update(new PreparedStatementCreator() {
                    @Override
                    public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                        PreparedStatement ps;
                        ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                        int i = 1;
                        for (Object param : queryParams) {
                            ps.setObject(i++, param);
                        }
                        return ps;
                    }
                }, keyHolder);
                if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

            if (rowCount > 0) {

                KeyInfo keyInfoPer = new KeyInfo(keyInfo.getExpire(), table);
                keyInfoPer.setIndexes(indexes);
                keyInfoPer.setIsNew(true);
                keyInfos.add(keyInfoPer);

                if (autoIncKey != null && keyHolder.getKey() == null) {
                    LOGGER.error("failed to get auto increment id from query");
                }
                if (autoIncKey != null & keyHolder.getKey() != null) {
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(autoIncKey, keyValue);
                    if (enableRedisCache) {
                        AppCtx.getLocalCache().putData(key, map, keyInfoPer);
                    }
                    if (enableRedisCache) {
                        AppCtx.getRedisRepo().save(context, new KvPairs(pair), new AnyKey(keyInfoPer));
                    }
                }
                Query.fetchClauseParams(context, keyInfoPer, map, pair.getId());
            }
        }

        AppCtx.getKeyInfoRepo().save(context, pairs, new AnyKey(keyInfos));

        return true;
    }

    @Override
    public boolean update(Context context, KvPairs pairs, AnyKey anyKey) {

        KvPair pair = pairs.getPair();
        String key = pair.getId();
        KeyInfo keyInfo = anyKey.getKey();
        String table = keyInfo.getTable();

        LOGGER.trace("update: " + key + " table: " + table);

        Map<String, Object> map = pair.getData();
        if (map == null || map.size() == 0) {
            String msg = "data is null or empty";
            LOGGER.error(msg);
            throw new ServerErrorException(context, msg);
        }

        if (table != null && Query.readyForUpdate(context, pair, keyInfo)) {

            if (Query.fetchClauseParams(context, keyInfo, map, key)) {

                String clause = keyInfo.getClause();
                List<Object> params = keyInfo.getParams();


                List<Object> queryParams = new ArrayList<Object>();
                String updates = "";
                for (Map.Entry<String, Object> entry: map.entrySet()) {
                    queryParams.add(entry.getValue());
                    if (updates.length() != 0) updates += ", ";
                    updates += entry.getKey() + " = ?";
                }

                queryParams.addAll(params);
                String sql = "update " + table + " set " + updates + " where " + clause + " limit 1";

                StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
                try {
                    if (jdbcTemplate.update(sql, queryParams.toArray()) > 0) {
                        if (stopWatch != null) stopWatch.stopNow();

                        LOGGER.debug("update to " + table + " ok for " + key);

                        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

                        final QueryInfo finalQueryInfo = keyInfo.getQueryInfo();
                        if (finalQueryInfo != null) {
                            keyInfo.setQueryInfo(null);
                            Utils.getExcutorService().submit(() -> {
                                Thread.yield();
                                Query.save(context, finalQueryInfo);
                            });
                        }

                        return true;
                    }
                    if (stopWatch != null) stopWatch.stopNow();

                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    e.printStackTrace();
                    if (enableDbFallback) {
                        context.logTraceMessage(msg);
                    } else {
                        throw new BadRequestException(context, msg);
                    }
                }
            }
            String msg = "update fallbacked to default table for " + pair.getId();
            LOGGER.warn(msg);
            if (enableDbFallback) {
                context.logTraceMessage(msg);
            } else {
                throw new BadRequestException(context, "failed to update database");
            }
        }

        // otherwise save to default table
        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        AppCtx.getKvPairRepo().save(pair);
        if (stopWatch != null) stopWatch.stopNow();

        setUseDefaultTable(keyInfo);

        AppCtx.getKeyInfoRepo().save(context, pairs, anyKey);

        return true;
    }

    @Override
    public boolean delete(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("delete pairs(" + pairs.size() + ") anyKey(" + anyKey.size() + "): " + anyKey.getAny().toString());

        Boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();
            KeyInfo keyInfo = anyKey.getAny(i);
            String table = keyInfo.getTable();

            if (table != null && Query.readyForDelete(context, pair, keyInfo)) {

                String clause = keyInfo.getClause();
                List<Object> params = keyInfo.getParams();

                String sql = "delete from " + table + " where " + clause + " limit 1";

                StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
                try {
                    if (jdbcTemplate.update(sql, params.toArray()) > 0) {
                        if (stopWatch != null) stopWatch.stopNow();

                        LOGGER.debug("delete from " + table + " ok for " + key);

                        AppCtx.getKeyInfoRepo().delete(context, pairs, true);

                        continue;

                    } else if (stopWatch != null) {

                        stopWatch.stopNow();
                    }

                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    e.printStackTrace();
                    context.logTraceMessage(msg);
                }

                String msg = "delete fallbacked to default table for " + pair.getId();
                LOGGER.warn(msg);
                if (enableDbFallback) {
                    context.logTraceMessage(msg);
                } else {
                    allOk = false;
                    continue;
                }
            }

            // otherwise save to default table
            //
            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
            AppCtx.getKvPairRepo().delete(pair);
            if (stopWatch != null) stopWatch.stopNow();

            setUseDefaultTable(keyInfo);

            AppCtx.getKeyInfoRepo().delete(context, pairs, true);
        }

        LOGGER.trace("delete returns " + allOk);

        return allOk;
    }

    private void setUseDefaultTable(KeyInfo keyInfo) {
        keyInfo.setQueryKey(null);
        keyInfo.setTable(null);
        keyInfo.setClause(null);
        keyInfo.setParams(null);
        keyInfo.setQueryInfo(null);
    }

    private Map<String, Object> convertDbMap(Map<String, Object> columns, Map<String, Object> dbMap) {

        for (Map.Entry<String, Object> entry: dbMap.entrySet()) {

            String key = entry.getKey();
            Map<String, Object> attributes = (Map<String, Object>) columns.get(key);
            if (attributes == null) {
                continue;
            }
            String type = (String) attributes.get("Type");
            if (type == null) {
                continue;
            }
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (Arrays.asList("timestamp", "datetime", "date", "time", "year(4)").contains(type)) {
                assert value instanceof Date : "convertDbMap " + key + " is not instance of Date";
                dbMap.put(key, AppCtx.getDbaseOps().formatDate(type, (Date) value));
            }
        }
        return dbMap;
    }

    private int getLimt(Context context, KvPairs pairs, KeyInfo keyInfo) {

        Integer queryLimit = keyInfo.getQueryLimit();
        int size = pairs.size();
        int limit = 0;
        if (queryLimit != null || size > 0) {
            if (size == 0)  {
                limit = queryLimit;
            } else if (queryLimit != null && size > queryLimit) {
                limit = queryLimit;
            } else {
                limit = size;
            }
        }
        return limit;
    }
}
