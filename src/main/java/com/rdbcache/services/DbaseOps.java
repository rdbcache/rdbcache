/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class DbaseOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseOps.class);

    public void setup() {
        setDefaultToDbTimeZone();
        cacheAllTablesInfo();
    }

    public void setDefaultToDbTimeZone() {

        String timezone = fetchDbTimeZone();

        if (timezone == null) {
            LOGGER.error("failed to get database timezone");
            timezone = "UTC";
        }

        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    synchronized public void logTraceMessage(String traceId, String message, StackTraceElement[] trace) {

        Map<String, Object> log = new LinkedHashMap<String, Object>();
        log.put("timestamp", System.currentTimeMillis());
        log.put("message", message);

        String simpleTrace = "";
        for (int i = 2; i < 6; i++) {
            StackTraceElement target = trace[i];
            String methodName = target.getMethodName();
            String filename = target.getFileName();
            int line = target.getLineNumber();
            if ( i > 2 && line <= 0) break;
            if (simpleTrace.length() > 0) {
                simpleTrace += "<-";
            }
            simpleTrace += methodName+"@"+filename+"#"+Integer.toString(line);
        }
        log.put("trace", simpleTrace);

        KvIdType idType = new KvIdType(traceId, "trace");
        KvPair pair = AppCtx.getKvPairRepo().findOne(idType);
        if (pair == null) {
            pair = new KvPair(idType);
        }

        Map<String, Object> map = pair.getData();
        String logId = Long.toString(map.size());
        map.put(logId, log);
        AppCtx.getKvPairRepo().save(pair);
    }

    public Map<String, Object> getTableList(Context context) {

        Map<String, Object> map = AppCtx.getLocalCache().get("table_list::");
        if (map != null) {
            return map;
        }
        map = AppCtx.getLocalCache().put("table_list::", Cfg.getTableInfoCacheTTL() * 1000L, () -> {
            Map<String, Object> map2 = fetchTableList(context);
            if (map2 == null) {
                LOGGER.error("failed to get table list");
            }
            return map2;
        });
        return map;
    }

    public Map<String, Object> getTableColumns(Context context, String table) {

        Map<String, Object> map = map = (Map<String, Object>) AppCtx.getLocalCache().get("table_columns::" + table);
        if (map != null) {
            return map;
        }

        Object object = AppCtx.getLocalCache().put("table_columns::" + table, Cfg.getTableInfoCacheTTL() * 1000L, () -> {
            Map<String, Object> map2 = fetchTableColumns(context, table);
            if (map2 == null) {
                String msg = "failed to get table columns";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return map2;
        });
        map = (Map<String, Object>) object;

        return map;
    }

    public Map<String, Object> getTableIndexes(Context context, String table) {

        Map<String, Object> map = (Map<String, Object>) AppCtx.getLocalCache().get("table_indexes::" + table);
        if (map != null) {
            return map;
        }

        Object object = AppCtx.getLocalCache().put("table_indexes::" + table, Cfg.getTableInfoCacheTTL() * 1000L, () -> {
            Map<String, Object> map2 = fetchTableIndexes(context, table);
            if (map2 == null) {
                String msg = "failed to get table indexes";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return map2;
        });
        map = (Map<String, Object>) object;

        return map;
    }

    public void cacheAllTablesInfo() {

        Map<String, Object> map = getTableList(null);
        List<String> tables = (List<String>) map.get("tables");
        for (String table: tables) {
            getTableColumns(null, table);
            getTableIndexes(null, table);
        }
    }

    public String getFieldType(Context context, Map<String, Object> tableInfo, String field) {
        
        Map<String, Object> attributes = (Map<String, Object>) tableInfo.get(field);
        if (attributes == null) {
            return null;
        }
        return (String) attributes.get("Type");
    }

    public String formatDate(String type, Date date) {
        if (type.equals("year(4)") || type.equals("year")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
            return sdf.format(date);
        } else if (type.equals("time")) {
            SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
            return sdf.format(date);
        } else if (type.equals("date")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return sdf.format(date);
        } else if (type.equals("datetime")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            return sdf.format(date);
        } else if (type.equals("timestamp")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            return sdf.format(date);
        }
        assert false : "not supported type " + type;
        return null;
    }

    // get database time zone
    //
    public String fetchDbTimeZone() {

        try {
            String sql = "SELECT @@global.time_zone as GLOBAL, @@system_time_zone as SYSTEM, @@session.time_zone as SESSION";

            List<Map<String, Object>> list = AppCtx.getJdbcTemplate().queryForList(sql);
            if (list.size() == 1) {
                Map<String, Object> timezones = list.get(0);
                String timezone = (String) timezones.get("SESSION");
                if (timezone.equals("SYSTEM")) {
                    timezone = (String) timezones.get("SYSTEM");
                    if (timezone.equals("GLOBAL")) {
                        timezone = (String) timezones.get("GLOBAL");
                    }
                }
                return timezone;
            }
        } catch (Exception e) {
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
        }
        return null;
    }

    // get list of tables
    //
    public Map<String, Object> fetchTableList(Context context) {

        String sql = "SHOW TABLES";

        StopWatch stopWatch = null;
        if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
        try {
            List<String> tables = AppCtx.getJdbcTemplate().queryForList(sql, String.class);
            if (stopWatch != null) stopWatch.stopNow();

            Map<String, Object> map = new HashMap<>();
            map.put("tables", tables);
            return map;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        throw new ServerErrorException("failed to get database table list");
    }

    // get table columns info
    //
    public Map<String, Object> fetchTableColumns(Context context, String table) {

        if (table != null) {

            Map<String, Object> map = new LinkedHashMap<String, Object>();
            String sql = "SHOW COLUMNS FROM " + table;

            StopWatch stopWatch = null;
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            try {
                List<Map<String, Object>> columns = AppCtx.getJdbcTemplate().queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();

                for (int i = 0; i < columns.size(); i++) {
                    Map<String, Object> column = columns.get(i);
                    String field = (String) column.get("Field");
                    column.remove("Field");
                    map.put(field, column);
                }
                return map;
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
                e.printStackTrace();
            }
        }
        throw new ServerErrorException("failed to get database table columns");
    }

    // get table indexes info
    //
    public Map<String, Object> fetchTableIndexes(Context context, String table) {

        if (table != null) {

            String sql = "SHOW INDEXES FROM " + table + "  WHERE Non_unique = false";

            StopWatch stopWatch = null;
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            try {
                List<Map<String, Object>> indexes = AppCtx.getJdbcTemplate().queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();

                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (int i = 0; i < indexes.size(); i++) {
                    Map<String, Object> index = indexes.get(i);
                    String key_name = (String) index.get("Key_name");
                    List<String> columns = (List<String>) map.get(key_name);
                    if (columns == null) {
                        columns = new ArrayList<String>();
                        map.put(key_name, columns);
                    }
                    columns.add((String) index.get("Column_name"));
                }
                return map;
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
                e.printStackTrace();
            }
        }
        throw new ServerErrorException("failed to get database table indexes");
    }
}
