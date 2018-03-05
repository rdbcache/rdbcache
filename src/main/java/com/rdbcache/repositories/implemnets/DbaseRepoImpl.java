/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories.implemnets;

import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.*;
import com.rdbcache.models.*;
import com.rdbcache.repositories.DbaseRepo;
import com.rdbcache.services.AsyncOps;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Repository
public class DbaseRepoImpl implements DbaseRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseRepoImpl.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public boolean findOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("findOne: " + key + " table: " + table);

        if (keyInfo.readyForQuery(context)) {
            String clause = keyInfo.getClause();
            List<Object> params = keyInfo.getParams();
            Map<String, Object> columns = keyInfo.fetchColumns(context);
            List<String> indexes = keyInfo.fetchIndexes(context);

            String sql = "select * from " + table + " where " + clause + " limit 1";

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            try {
                List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
                if (stopWatch != null) stopWatch.stopNow();

                if (list.size() == 1) {

                    LOGGER.debug("found from " + table + " for " + key);

                    Map<String, Object> map = convertDbMap(columns, list.get(0));
                    pair.setData(map);

                    keyInfo.fetchPKClauseParams(context, map, key);

                    AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);

                    final Query finalQuery = keyInfo.getQuery();
                    if (finalQuery != null) {
                        keyInfo.setQuery(null);
                        AsyncOps.getExecutor().submit(() -> {
                            Thread.yield();
                            finalQuery.save(context);
                        });
                    }

                    return true;
                }
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                if (Cfg.getEnableDbFallback()) {
                    context.logTraceMessage(msg);
                } else {
                    throw new BadRequestException(context, msg);
                }
            }
        }

        // otherwise find it from default table
        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(key));
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair == null) {
            return false;
        }

        setUseDefaultTable(keyInfo);
        pair.setData(dbPair.getData());

        return true;
    }

    private boolean kvFindAll(Context context, KeyInfo keyInfo) {

        Query query = keyInfo.getQuery();
        if (query == null) {
            LOGGER.debug("no query");
            return false;
        }
        Map<String, Condition> conditions = query.getConditions();
        if (conditions == null ||
                conditions.size() != 1 ||
                !conditions.containsKey("key")) {
            LOGGER.debug("no conditions or not key only");
            return false;
        }
        Condition condition = conditions.get("key");
        if (condition == null ||
                condition.size() != 1 ||
                !condition.containsKey("=")) {
            LOGGER.debug("only = is supported");
            return false;
        }
        List<String> keys = condition.get("=");
        if (keys == null || keys.size() == 0 ) {
            return false;
        }
        List<KvIdType> idTypes = new ArrayList<KvIdType>();
        for (String key: keys) {
            KvIdType idType = new KvIdType(key, "data");
            idTypes.add(idType);
        }

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findAll");
        Iterable<KvPair> dbPairs = AppCtx.getKvPairRepo().findAll(idTypes);
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPairs == null && !dbPairs.iterator().hasNext()) {
            return false;
        }

        List<KvPair> pairs = context.getPairs();
        for (KvPair dbPair: dbPairs) {
            pairs.add(dbPair);
        }

        return true;
    }

    @Override
    public boolean findAll(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        String table = keyInfo.getTable();

        LOGGER.trace("findAll: " + pairs.size() + " table: " + table);

        if (table == null) {
            return kvFindAll(context, keyInfo);
        }

        Map<String, Object> columns = keyInfo.fetchColumns(context);
        List<String> indexes = keyInfo.fetchIndexes(context);

        if (!keyInfo.readyForQuery(context)) {
            return false;
        }

        String clause = keyInfo.getClause();
        List<Object> params = keyInfo.getParams();

        String sql = "select * from " + table;

        if (clause != null && clause.length() > 0) {
            sql += " where " + clause;
        }

        int limit = getLimt(context, keyInfo);
        if (limit > 0) {
            sql += " limit " + limit;
        }

        StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
            if (stopWatch != null) stopWatch.stopNow();


            if (list.size() > 0) {

                LOGGER.debug("found " + list.size() + " record(s) from " + table);

                int size = pairs.size();
                List<KeyInfo> keyInfos = new ArrayList<KeyInfo>();
                KvPair pair = null;
                for (int i = 0; i < list.size(); i++) {

                    if (i >= size) {
                        pair = new KvPair(Utils.generateId());
                        pairs.add(pair);
                    } else {
                        pair = pairs.get(i);
                    }
                    Map<String, Object> map = convertDbMap(columns, list.get(i));
                    pair.setData(map);

                    KeyInfo keyInfoPer = new KeyInfo(keyInfo.getExpire(), table);
                    keyInfos.add(keyInfoPer);

                    keyInfoPer.setIndexes(indexes);
                    keyInfoPer.setColumns(columns);
                    keyInfoPer.fetchPKClauseParams(context, map, pair.getId());
                    keyInfoPer.setQuery(null);
                }
                AppCtx.getKeyInfoRepo().saveAll(context, keyInfos);

                keyInfo.setIsNew(false);
                final Query finalQuery = keyInfo.getQuery();
                if (finalQuery!= null) {
                    keyInfo.setQuery(null);
                    AsyncOps.getExecutor().submit(() -> {
                        Thread.yield();
                        finalQuery.save(context);
                    });
                }
                return true;
            }
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            context.logTraceMessage(msg);
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean saveOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
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
        Context dbCtx = context.getCloneWith(key);
        if (!findOne(dbCtx, keyInfo)) {
            return insertOne(context, keyInfo);
        }

        KvPair dbPair = dbCtx.getPair();
        String dbValue = dbPair.getValue();
        String value = pair.getValue();
        if (value != null && value.equals(dbValue)) {
            LOGGER.trace("identical as string");
            return true;
        }

        Map<String, Object> dbMap = dbPair.getData();
        if (dbMap != null && dbMap.size() > 0) {

            if (table != null) {

                Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
                if (!Utils.MapChangesAfterUpdate(map, dbMap, todoMap)) {
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

            if (!keyInfo.hasStdPKClause(context)) {

                keyInfo.fetchPKClauseParams(context, dbMap, key);
                AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);

                final Query finalQuery = keyInfo.getQuery();
                if (finalQuery != null) {
                    AsyncOps.getExecutor().submit(() -> {
                        Thread.yield();
                       finalQuery.save(context);
                    });
                }
            }
        }

        return updateOne(context, keyInfo);
    }

    @Override
    public boolean saveAll(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        String table = keyInfo.getTable();

        LOGGER.trace("saveAll: #pairs = " + pairs.size() + " table: " + table);

        List<String> indexes = keyInfo.fetchIndexes(context);

        final Query finalQuery = keyInfo.getQuery();
        if (finalQuery != null) {
            keyInfo.setQuery(null);
            AsyncOps.getExecutor().submit(() -> {
                Thread.yield();
                finalQuery.save(context);
            });
        }

        for (KvPair pair: pairs) {

            String key = pair.getId();
            KeyInfo keyInfoPer = keyInfo.clone();

            Map<String, Object> map = pair.getData();
            if (map == null || map.size() == 0) {
                String msg = "data is null or empty";
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                continue;
            }

            // get it from database
            Context dbCtx = context.getCloneWith(key);
            if (!findOne(dbCtx, keyInfoPer)) {
                insertOne(context.getCloneWith(pair), keyInfoPer);
                continue;
            }

            KvPair dbPair = dbCtx.getPair();
            String dbValue = dbPair.getValue();
            String value = pair.getValue();
            if (value != null && value.equals(dbValue)) {
                LOGGER.trace("identical as string");
                continue;
            }

            Map<String, Object> dbMap = dbPair.getData();

            if (dbMap != null && dbMap.size() > 0) {

                if (table != null) {

                    Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
                    if (!Utils.MapChangesAfterUpdate(map, dbMap, todoMap)) {
                        String msg = "unknown field found in input";
                        LOGGER.error(msg);
                        throw new BadRequestException(context, msg);
                    }

                    // identical map
                    if (todoMap.size() == 0) {
                        LOGGER.trace("identical as map");
                        continue;
                    }
                    pair.setData(todoMap);
                } else if (Utils.isMapEquals(map, dbMap)) {

                    LOGGER.trace("identical as map match");
                    continue;
                }
            }

            keyInfoPer.setIndexes(indexes);

            if (!keyInfoPer.hasStdPKClause(context)) {
                keyInfoPer.fetchPKClauseParams(context, dbMap, key);
                AppCtx.getKeyInfoRepo().saveOne(context, keyInfoPer);
            }

            updateOne(context, keyInfoPer);
        }
        return true;
    }

    @Override
    public boolean insertOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("insertOne: " + key + " table: " + table);

        Map<String, Object> map = pair.getData();
        if (map == null || map.size() == 0) {
            String msg = "data is null or empty";
            LOGGER.error(msg);
            throw new ServerErrorException(context, msg);
        }

        if (keyInfo.readyForInsert(context)) {

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
                if (Cfg.getEnableDbFallback()) {
                    context.logTraceMessage(msg);
                } else {
                    throw new BadRequestException(context, msg);
                }
            }

            if (rowCount > 0) {

                List<String> indexes = keyInfo.fetchIndexes(context);
                if (keyHolder.getKey() != null) {
                    String primaryKey = indexes.get(0);
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(primaryKey, keyValue);
                    AppCtx.getLocalCache().putData(key, map, keyInfo);
                }
                keyInfo.fetchPKClauseParams(context, map, key);
                AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);

                keyInfo.setQuery(null);
                LOGGER.debug("insert ok: " + table + " for " + pair.getId());
                return true;
            }

            if (Cfg.getEnableDbFallback()) {
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

        return true;
    }

    @Override
    public boolean insertAll(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        String table = keyInfo.getTable();

        LOGGER.trace("insertAll: #pairs = " + pairs.size() + " table: " + table);

        if (table == null) {

            StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findAll");
            try {
                AppCtx.getKvPairRepo().save(pairs);
                if (stopWatch != null) stopWatch.stopNow();
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

        if (!keyInfo.readyForInsert(context)) {
            return false;
        }

        List<String> indexes = keyInfo.fetchIndexes(context);

        List<KeyInfo> keyInfos = new ArrayList<KeyInfo>();

        for (KvPair pair: pairs) {

            String key = pair.getId();
            Map<String, Object> map = pair.getData();

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
                keyInfos.add(keyInfoPer);
                keyInfoPer.setIndexes(indexes);

                keyInfo.setIsNew(false);
                if (keyHolder.getKey() != null) {
                    String primaryKey = indexes.get(0);
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(primaryKey, keyValue);
                    AppCtx.getLocalCache().putData(key, map, keyInfoPer);
                }
                keyInfoPer.fetchPKClauseParams(context, map, pair.getId());
            }
        }

        AppCtx.getKeyInfoRepo().saveAll(context, keyInfos);

        return true;
    }

    @Override
    public boolean updateOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("updateOne: " + key + " table: " + table);

        Map<String, Object> map = pair.getData();
        if (map == null || map.size() == 0) {
            String msg = "data is null or empty";
            LOGGER.error(msg);
            throw new ServerErrorException(context, msg);
        }

        if (keyInfo.readyForUpdate(context)) {

            List<String> indexes = keyInfo.fetchIndexes(context);

            if (keyInfo.fetchPKClauseParams(context, map, key)) {

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

                        return true;
                    }
                    if (stopWatch != null) stopWatch.stopNow();

                } catch (Exception e) {
                    if (stopWatch != null) stopWatch.stopNow();

                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    e.printStackTrace();
                    if (Cfg.getEnableDbFallback()) {
                        context.logTraceMessage(msg);
                    } else {
                        throw new BadRequestException(context, msg);
                    }
                }
            }
            String msg = "update fallbacked to default table for " + pair.getId();
            LOGGER.warn(msg);
            if (Cfg.getEnableDbFallback()) {
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

        return true;
    }

    @Override
    public boolean deleteOne(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("deleteOne: " + key + " table: " + table);

        if (keyInfo.readyForDelete(context)) {

            String clause = keyInfo.getClause();
            List<Object> params = keyInfo.getParams();

            String sql = "delete from " + table + " where " + clause + " limit 1";

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
            try {
                if (jdbcTemplate.update(sql, params.toArray()) > 0) {
                    if (stopWatch != null) stopWatch.stopNow();

                    LOGGER.debug("delete from " + table + " ok for " + key);

                    return true;
                }
                else if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
                if (Cfg.getEnableDbFallback()) {
                    context.logTraceMessage(msg);
                } else {
                    throw new BadRequestException(context, msg);
                }
            }
            String msg = "delete fallbacked to default table for " + pair.getId();
            LOGGER.warn(msg);
            if (Cfg.getEnableDbFallback()) {
                context.logTraceMessage(msg);
            } else {
                throw new BadRequestException(context, "failed to delete from database");
            }
        }

        // otherwise save to default table
        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        AppCtx.getKvPairRepo().delete(pair);
        if (stopWatch != null) stopWatch.stopNow();

        setUseDefaultTable(keyInfo);

        return true;
    }

    private void setUseDefaultTable(KeyInfo keyInfo) {
        keyInfo.setQueryKey(null);
        keyInfo.setTable(null);
        keyInfo.setClause(null);
        keyInfo.setParams(null);
        keyInfo.setQuery(null);
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

    private int getLimt(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
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
