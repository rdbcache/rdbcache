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

    public boolean ifSelectOk() {

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

        QueryInfo queryInfo = keyInfo.getQueryInfo();
        if (queryInfo != null) {
            keyInfo.setQueryInfo(null);
            Utils.getExcutorService().submit(() -> {
                Thread.yield();
                Parser.saveQuery(context, queryInfo);
            });
        }

        return true;
    }

    public boolean executeSelect() {

        KeyInfo keyInfo = anyKey.getAny();
        String table = keyInfo.getTable();

        LOGGER.trace("sql: " + sql);
        LOGGER.trace("params: " + params.toString());

        StopWatch stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, params.toArray());
            if (stopWatch != null) stopWatch.stopNow();

            if (list != null && list.size() > 0) {

                Map<String, Object> columns = keyInfo.getColumns();

                for (int i = 0; i < list.size(); i++) {

                    KvPair pair = pairs.getAny(i);
                    keyInfo = anyKey.getAny(i);

                    pair.setData(AppCtx.getDbaseOps().convertDbMap(columns, list.get(i)));

                    if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {
                        String msg = "failed when prepareStandardClauseParams for " + pair.getId();
                        LOGGER.error(msg);
                        context.logTraceMessage(msg);
                    }

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

    public boolean ifInsertOk() {

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

        return true;
    }

    public boolean executeInsert(boolean enableLocal, boolean enableRedis) {

        params = new ArrayList<>();
        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair  = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);

            String table = keyInfo.getTable();
            Map<String, Object> map = pair.getData();

            String fields = "", values = "";
            params.clear();
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
                    String keyValue = String.valueOf(keyHolder.getKey());
                    map.put(autoIncKey, keyValue);
                    if (enableLocal) {
                        AppCtx.getLocalCache().putData(pair, keyInfo);
                    }
                    if (enableRedis) {
                        AppCtx.getRedisRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));
                    }
                }

                if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {
                    String msg = "failed when prepareStandardClauseParams for " + pair.getId();
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                }

                LOGGER.trace("inserted " + pair.getId() + " into " + table);

            } else {

                allOk = false;

                LOGGER.warn("failed to insert " + pair.getId() + " into " + table);
            }
        }

        return allOk;
    }

    public boolean ifUpdateOk() {

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

        return true;
    }

    public boolean executeUpdate() {

        params = new ArrayList<>();

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair  = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);

            String table = keyInfo.getTable();

            if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {

                allOk = false;

                String msg = "failed to update when calling prepareStandardClauseParams for " + pair.getId();
                LOGGER.error(msg);
                context.logTraceMessage(msg);

                continue;
            }

            Map<String, Object> map = pair.getData();

            params.clear();
            String updates = "";
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                params.add(entry.getValue());
                if (updates.length() != 0) updates += ", ";
                updates += entry.getKey() + " = ?";
            }

            params.addAll(keyInfo.getParams());
            String clause = keyInfo.getClause();

            sql = "update " + table + " set " + updates + " where " + clause + " limit 1";

            LOGGER.trace("sql: " + sql);
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

    public boolean ifDeleteOk() {

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

        return true;
    }

    public boolean executeDelete() {

        boolean allOk = true;

        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair  = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);

            String table = keyInfo.getTable();

            if (!Parser.prepareStandardClauseParams(context, pair, keyInfo)) {

                String msg = "failed to delete when calling prepareStandardClauseParams for " + pair.getId();
                LOGGER.error(msg);
                context.logTraceMessage(msg);

                continue;
            }
            params = keyInfo.getParams();
            String clause =  keyInfo.getClause();

            sql = "delete from " + table + " where " + clause + " limit 1";

            LOGGER.trace("sql: " + sql);
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
}
