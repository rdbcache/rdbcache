/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.queries;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Query {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    private Context context;

    private JdbcTemplate jdbcTemplate;

    private KvPairs pairs;

    private AnyKey anyKey;

    private String sql;

    private List<Object> params;

    public Query(Context context, JdbcTemplate jdbcTemplate, KvPairs pairs, AnyKey anyKey) {
        this.context = context;
        this.jdbcTemplate = jdbcTemplate;
        this.pairs = pairs;
        this.anyKey = anyKey;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    public boolean pepareSelect() {

        Assert.isTrue(anyKey.size() == 1, "anyKey.size() = " +
                anyKey.size() + ", select only supports anyKey size == 1");

        KeyInfo keyInfo = anyKey.getAny();

        String table = keyInfo.getTable();
        if (table == null) {
            return false;
        }

        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }

        String clause = keyInfo.getClause();
        params = keyInfo.getParams();

        if (clause == null || clause.length() == 0 || params == null || params.size() == 0) {

            boolean ready = false;
            KvPair pair = pairs.getPair();
            if (Parser.prepareQueryClauseParams(context, pair, keyInfo)) {
                ready = true;
            } else if (keyInfo.getQueryLimit() != null) {
                ready = true;
            } else if (Parser.prepareStandardClauseParams(context, pair, keyInfo)) {
                ready = true;
            }
            if (!ready) {
                return false;
            }

            clause = keyInfo.getClause();
            params = keyInfo.getParams();
        }

        int limit = 1;
        if (context.isBatch()) {
            limit = getLimt();
        }

        sql = "select * from " + table;

        if (clause != null && clause.length() > 0) {
            sql += " where " + clause;
        }
        if (limit != 0) {
            sql += " limit " + limit;
        }

        keyInfo.setIsNew(false);

        QueryInfo queryInfo = keyInfo.getQueryInfo();
        if (queryInfo != null) {
            keyInfo.setQueryInfo(null);
            Utils.getExcutorService().submit(() -> {
                Thread.yield();
                Parser.save(context, queryInfo);
            });
        }

        LOGGER.trace("sql: " + sql);

        return true;
    }

    public boolean executeSelect() {

        KeyInfo keyInfo = anyKey.getAny();
        String table = keyInfo.getTable();

        LOGGER.trace("params: " + params.toString());

        StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
            if (stopWatch != null) stopWatch.stopNow();

            if (list != null && list.size() > 0) {

                Map<String, Object> columns = keyInfo.getColumns();

                anyKey.clear();
                for (int i = 0; i < list.size(); i++) {

                    if (i == pairs.size()) {
                        pairs.add(new KvPair("*"));
                    }

                    KvPair pair = pairs.get(i);
                    pair.setData(convertDbMap(columns, list.get(i)));

                    Parser.prepareStandardClauseParams(context, pair, keyInfo);

                    KeyInfo keyInfoNew = keyInfo.clone();
                    keyInfoNew.setIsNew(true);
                    anyKey.add(keyInfoNew);

                    LOGGER.trace("found " + pair.getId() + " from " + table);
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

    public boolean prepareInsert() {

        Assert.isTrue(anyKey.size() == 1, "anyKey.size() = " +
                anyKey.size() + ", insert only supports anyKey size == 1");

        KeyInfo keyInfo = anyKey.getAny();

        String table = keyInfo.getTable();
        if (table == null) {
            return false;
        }

        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }

        KvPair pair = pairs.getPair();
        Map<String, Object> map = pair.getData();

        params = new ArrayList<>();
        String fields = "", values = "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            params.add(entry.getValue());
            if (fields.length() != 0) {
                fields += ", ";
                values += ", ";
            }
            fields += entry.getKey();
            values += "?";
        }

        sql = "insert into " + table + " (" + fields + ") values (" + values + ")";

        LOGGER.trace("sql: " + sql);

        return true;
    }

    public boolean executeInsert(boolean enableLocal, boolean enableRedis) {

        KeyInfo keyInfo = anyKey.getAny();
        String table = keyInfo.getTable();

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair  = pairs.get(i);
            Map<String, Object> map = pair.getData();

            if (i > 0) {
                params.clear();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    params.add(entry.getValue());
                }
            }

            LOGGER.trace("params: " + params.toString());

            int rowCount = 0;
            KeyHolder keyHolder = new GeneratedKeyHolder();

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
            try {
                rowCount = jdbcTemplate.update(new PreparedStatementCreator() {

                    @Override
                    public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                        PreparedStatement ps;
                        ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                        int i = 1;
                        for (Object param : params) {
                            ps.setObject(i++, param);
                        }
                        return ps;
                    }
                }, keyHolder);
                if (stopWatch != null) stopWatch.stopNow();

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                allOk = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
            }

            if (rowCount > 0) {

                String autoIncKey = AppCtx.getDbaseOps().getTableAutoIncColumn(context, table);
                if (autoIncKey != null && keyHolder.getKey() != null) {
                    String key = pair.getId();
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(autoIncKey, keyValue);
                    if (enableLocal) {
                        AppCtx.getLocalCache().putData(key, map, keyInfo);
                    }
                    if (enableRedis) {
                        AppCtx.getRedisRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));
                    }
                }

                Parser.prepareStandardClauseParams(context, pair, keyInfo);

                KeyInfo keyInfoNew = keyInfo.clone();
                keyInfoNew.setIsNew(true);
                if (i < anyKey.size()) {
                    anyKey.setKey(i, keyInfoNew);
                } else {
                    anyKey.add(keyInfoNew);
                }
                LOGGER.trace("inserted " + pair.getId() + " into " + table);

            } else {

                allOk = false;

                LOGGER.warn("failed to insert " + pair.getId() + " into " + table);

                if (i == anyKey.size()) {
                    KeyInfo keyInfoNew = new KeyInfo(keyInfo.getExpire());
                    keyInfoNew.setIsNew(true);
                    keyInfoNew.setQueryKey(null);
                    anyKey.add(keyInfoNew);
                }
            }
        }

        return allOk;
    }

    public boolean prepareUpdate() {

        Assert.isTrue(anyKey.size() == pairs.size(), anyKey.size() + " != " +
                pairs.size() + ", update only supports anyKey size == pairs size");

        KeyInfo keyInfo = anyKey.getAny();

        String table = keyInfo.getTable();
        if (table == null) {
            return false;
        }

        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }

        KvPair pair  = pairs.getPair();
        Map<String, Object> map = pair.getData();

        if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {
            return false;
        }

        List<Object> queryParams = keyInfo.getParams();
        String clause = keyInfo.getClause();
        params = new ArrayList<>();

        String updates = "";
        for (Map.Entry<String, Object> entry: map.entrySet()) {
            params.add(entry.getValue());
            if (updates.length() != 0) updates += ", ";
            updates += entry.getKey() + " = ?";
        }

        params.addAll(queryParams);
        sql = "update " + table + " set " + updates + " where " + clause + " limit 1";

        LOGGER.trace("sql: " + sql);

        return true;
    }

    public boolean executeUpdate() {

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KeyInfo keyInfo = anyKey.getAny(i);
            String table = keyInfo.getTable();
            KvPair pair  = pairs.get(i);

            if (i > 0) {

                if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {

                    allOk = false;

                    String msg = "failed to update when calling prepareStandardClauseParams for " + pair.getId();
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);

                    continue;
                }

                List<Object> queryParams = keyInfo.getParams();
                Map<String, Object> map = pair.getData();
                params = new ArrayList<>();

                String updates = "";
                for (Map.Entry<String, Object> entry: map.entrySet()) {
                    params.add(entry.getValue());
                    if (updates.length() != 0) updates += ", ";
                    updates += entry.getKey() + " = ?";
                }

                params.addAll(queryParams);
                String clause = keyInfo.getClause();

                sql = "update " + table + " set " + updates + " where " + clause + " limit 1";

                LOGGER.trace("sql: " + sql);
            }

            LOGGER.trace("params: " + params.toString());

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.update");
            try {
                if (jdbcTemplate.update(sql, params.toArray()) > 0) {
                    if (stopWatch != null) stopWatch.stopNow();

                    LOGGER.trace("update " + pair.getId() + " from " + table);

                    continue;

                } else {
                    if (stopWatch != null) stopWatch.stopNow();

                    allOk = false;
                }

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                allOk = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

            keyInfo.setQueryKey(null);
        }

        return allOk;
    }

    public boolean prepareDelete() {

        Assert.isTrue(anyKey.size() == pairs.size(), anyKey.size() + " != " +
                pairs.size() + ", delete only supports anyKey size == pairs size");

        KeyInfo keyInfo = anyKey.getAny();

        String table = keyInfo.getTable();
        if (table == null) {
            return false;
        }

        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }

        KvPair pair  = pairs.getPair();
        Map<String, Object> map = pair.getData();

        if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {
            return false;
        }

        params = keyInfo.getParams();
        String clause =  keyInfo.getClause();

        sql = "delete from " + table + " where " + clause + " limit 1";

        LOGGER.trace("sql: " + sql);

        return true;
    }

    public boolean executeDelete() {

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KeyInfo keyInfo = anyKey.getAny(i);
            String table = keyInfo.getTable();
            KvPair pair  = pairs.get(i);

            if (i > 0) {

                if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {

                    String msg = "failed to delete when calling prepareStandardClauseParams for " + pair.getId();
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);

                    continue;
                }
                params = keyInfo.getParams();

                LOGGER.trace("sql: " + sql);
            }

            LOGGER.trace("params: " + params.toString());

            StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.delete");
            try {
                if (jdbcTemplate.update(sql, params.toArray()) > 0) {
                    if (stopWatch != null) stopWatch.stopNow();

                    LOGGER.trace("delete " + pair.getId() + " from " + table);

                    continue;

                } else {
                    if (stopWatch != null) stopWatch.stopNow();

                    allOk = false;
                }

            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                allOk = false;

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                e.printStackTrace();
            }

            keyInfo.setQueryKey(null);
        }

        return allOk;
    }

    private int getLimt() {

        KeyInfo keyInfo = anyKey.getAny();
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
}
