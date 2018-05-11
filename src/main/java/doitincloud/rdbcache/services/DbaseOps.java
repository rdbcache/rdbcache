/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.commons.helpers.Utils;
import doitincloud.commons.helpers.VersionInfo;
import doitincloud.rdbcache.models.*;
import doitincloud.rdbcache.queries.QueryInfo;
import doitincloud.rdbcache.repositories.KvPairRepo;
import doitincloud.rdbcache.repositories.MonitorRepo;
import doitincloud.rdbcache.supports.DbUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
public class DbaseOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseOps.class);

    private Long tableInfoCacheTTL = PropCfg.getTableInfoCacheTTL();

    private String databaseType = "h2";

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        tableInfoCacheTTL = PropCfg.getTableInfoCacheTTL();
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
        try {
            String driverName = AppCtx.getJdbcDataSource().getConnection().getMetaData().getDriverName();
            if (driverName.indexOf("MySQL") >= 0) {
                databaseType = "mysql";
            } else if (driverName.indexOf("H2") >= 0) {
                databaseType = "h2";
            } else {
                throw new ServerErrorException("database drive not support");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
        setDefaultToDbTimeZone();
        cacheAllTablesInfo();
    }

    public Long getTableInfoCacheTTL() {
        return tableInfoCacheTTL;
    }

    public void setTableInfoCacheTTL(Long tableInfoCacheTTL) {
        this.tableInfoCacheTTL = tableInfoCacheTTL;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    private static List<String> timeTypes = Arrays.asList("timestamp", "datetime", "date", "time", "year(4)");

    public Map<String, Object> convertDbMap(Map<String, Object> columns, Map<String, Object> dbMap) {

        for (Map.Entry<String, Object> entry : columns.entrySet()) {

            String key = entry.getKey();
            Object value = null;
            if (databaseType.equals("h2")) {
                String keyUpperCase = key.toUpperCase();
                if (!dbMap.containsKey(keyUpperCase)) {
                    continue;
                }
                value = dbMap.get(keyUpperCase);
                if (!key.equals(keyUpperCase)) {
                    dbMap.remove(keyUpperCase);
                    dbMap.put(key, value);
                }
            } else { //if (databaseType.equals("mysql")) {
                if (!dbMap.containsKey(key)) {
                    continue;
                }
                value = dbMap.get(key);

            }
            if (value == null) {
                continue;
            }
            Map<String, Object> attributes = (Map<String, Object>) entry.getValue();
            if (attributes == null) {
                continue;
            }
            String type = (String) attributes.get("type");
            if (type == null) {
                continue;
            }
            if (timeTypes.contains(type)) {
                Date dateValue = null;
                if (value instanceof Date) {
                    dateValue = (Date) value;
                } else if (value instanceof Long) {
                    dateValue = new Date((Long) value);
                } else if (value instanceof String) {
                    String strValue = (String) value;
                    if (strValue.length() > 10 && strValue.matches("[0-9]+")) {
                        dateValue = new Date(Long.valueOf(strValue));
                    }
                }
                if (dateValue != null) {
                    dbMap.put(key, DbUtils.formatDate(type, dateValue));
                }
            }
        }
        return dbMap;
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
        KvPair pair = AppCtx.getKvPairRepo().findById(idType);
        if (pair == null) {
            pair = new KvPair(idType);
        }

        Map<String, Object> map = pair.getData();
        String logId = Long.toString(map.size());
        map.put(logId, log);
        AppCtx.getKvPairRepo().save(pair);
    }

    public boolean saveMonitor(Context context) {

        Monitor monitor = context.getMonitor();
        if (monitor == null) {
            return false;
        }

        monitor.stopNow();
        VersionInfo versionInfo = AppCtx.getVersionInfo();
        if (versionInfo != null) {
            monitor.setBuiltInfo(versionInfo.getBriefInfo());
        }

        MonitorRepo monitorRepo = AppCtx.getMonitorRepo();
        if (monitorRepo == null) {
            return false;
        }

        monitorRepo.save(monitor);

       return true;
    }

    // save query info to database
    //
    public static boolean saveQuery(Context context, QueryInfo queryInfo) {

        if (context == null || context.getMonitor() == null  || !context.isMonitorEnabled()) {
            return false;
        }
        KvPairRepo kvPairRepo = AppCtx.getKvPairRepo();
        if (kvPairRepo == null) {
            return false;
        }

        KvPair queryPair = new KvPair(queryInfo.getKey(), "query", Utils.toMap(queryInfo));
        KvIdType idType = queryPair.getIdType();

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findById");
        KvPair dbPair = kvPairRepo.findById(idType);
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair != null) {
            if (queryPair.getData().equals(dbPair.getData())) {
                return true;
            }
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        kvPairRepo.save(queryPair);
        if (stopWatch != null) stopWatch.stopNow();

        return true;
    }

    public JdbcTemplate getJdbcTemplate(Context context, String table) {

        Map<String, Object> tables = getTablesMap(context);
        if (!tables.containsKey(table)) {
            throw new ServerErrorException("table "+table+" not found");
        }
        String type = (String) tables.get(table);
        if (type.equals("data")) {
            return AppCtx.getJdbcTemplate();
        }
        if (type.equals("system")) {
            return AppCtx.getSystemJdbcTemplate();
        }
        throw new ServerErrorException("JdbcTemplate " + type + " not supported");
    }

    public Map<String, Object> getTablesMap(Context context) {

        Map<String, Object> tablesMap = (Map<String, Object>) AppCtx.getCacheOps().get("tables_map");
        if (tablesMap != null) {
            return tablesMap;
        }

        tablesMap = (Map<String, Object>) AppCtx.getCacheOps().put("tables_map", tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> alltablesMap = fetchTableMap(context);
            if (alltablesMap == null) {
                LOGGER.error("failed to get table map");
                return null;
            }
            Map<String, Object> indexes = fetchTableUniqueIndexes(context);
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry: alltablesMap.entrySet()) {
                String table = entry.getKey();
                if (indexes.containsKey(table)) {
                    map.put(table, entry.getValue());
                } else {
                    LOGGER.warn("skip " + table + " table, due to no primary or unique index info");
                }
            }
            return map;
        });
        if (tablesMap == null) {
            throw new ServerErrorException("failed to get table list");
        }

        return tablesMap;
    }

    public Map<String, Object> getTableColumns(Context context, String table) {

        Assert.notNull(table, "table can not be null");
        Map<String, Object> columns = (Map<String, Object>) AppCtx.getCacheOps().get("tables_columns");
        if (columns != null) {
            return (Map<String, Object>) columns.get(table);
        }

        columns = (Map<String, Object>) AppCtx.getCacheOps().put("tables_columns", tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> map = fetchTableColumns(context);
            if (map == null) {
                String msg = "failed to get table columns";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return map;
        });

        if (columns == null) {
            throw new ServerErrorException("failed to get table columns");
        }

        return (Map<String, Object>) columns.get(table);
    }

    public String getTableAutoIncColumn(Context context, String table) {

        Map<String, Object> columns = (Map<String, Object>) AppCtx.getCacheOps().get("tables_auto_inc_columns");
        if (columns != null) {
            if (table == null) return null;
            return (String) columns.get(table);
        }

        columns = (Map<String, Object>) AppCtx.getCacheOps().put("tables_auto_inc_columns", tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> map = fetchTablesAutoIncrementColumns(context);
            if (map == null) {
                String msg = "failed to get table auto increment column map";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return map;
        });

        if (columns == null) {
            throw new ServerErrorException("failed to get table auto increment column map");
        }

        if (table == null) return null;
        return (String) columns.get(table);
    }

    public Map<String, Object> getTableIndexes(Context context, String table) {

        Assert.notNull(table, "table can not be null");
        Map<String, Object> map = (Map<String, Object>) AppCtx.getCacheOps().get("tables_indexes");
        if (map != null) {
            return (Map<String, Object>) map.get(table);
        }

        map = AppCtx.getCacheOps().put("tables_indexes", tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> indexes = fetchTableUniqueIndexes(context);
            if (indexes == null) {
                String msg = "failed to get table indexes";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return indexes;
        });
        if (map == null) {
            throw new ServerErrorException("failed to get table indexes");
        }

        return (Map<String, Object>) map.get(table);
    }

    public void cacheAllTablesInfo() {

        Map<String, Object> tables = getTablesMap(null);
        for (Map.Entry<String, Object> entry: tables.entrySet()) {
            String table = entry.getKey();
            getTableColumns(null, table);
            getTableIndexes(null, table);
            getTableAutoIncColumn(null, table);
        }
        getTableAutoIncColumn(null, null);
    }

    public String getFieldType(Context context, Map<String, Object> tableInfo, String field) {
        
        Map<String, Object> attributes = (Map<String, Object>) tableInfo.get(field);
        if (attributes == null) {
            return null;
        }
        return (String) attributes.get("type");
    }

    public void setDefaultToDbTimeZone() {

        String timezone = "UTC";
        /*
        if (databaseType.equals("mysql")) {
            String tz = fetchMysqlDbTimeZone();
            if (tz != null) {
                timezone = tz;
            }
        }
        */
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    // get database time zone from mysql
    //
    public String fetchMysqlDbTimeZone() {

        try {

            String sql = "SELECT @@global.time_zone as GLOBAL, @@system_time_zone as SYSTEM, @@session.time_zone as SESSION";

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
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

    // get list of tables from mysql
    //
    public Map<String, Object> fetchTableMap(Context context) {
        if (databaseType.equals("mysql")) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchMysqlTableMap(context, jdbcTemplate, map, "data");

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchMysqlTableMap(context, systemJdbcTemplate, map, "system");
            }
            return map;
        }
        if (databaseType.equals("h2")) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchH2TableMap(context, jdbcTemplate, map, "data");

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchH2TableMap(context, systemJdbcTemplate, map, "sysem");
            }
            return map;
        }
        throw new ServerErrorException("database type not supported");
    }

    private boolean fetchMysqlTableMap(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map, String type) {

        String sql = "SHOW TABLES";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<String> tables = jdbcTemplate.queryForList(sql, String.class);
            if (stopWatch != null) stopWatch.stopNow();
            for (String table: tables) {
                map.put(table, type);
            }
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return false;
    }

    private boolean fetchH2TableMap(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map , String type) {

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();
            for (Map<String, Object> mapTable: list) {
                if ("TABLE".equals(mapTable.get("TABLE_TYPE"))) {
                    String table = (String) mapTable.get("TABLE_NAME");
                    map.put(table, type);
                }
            }
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
       return false;
    }

    // get table columns info
    //
    public Map<String, Object> fetchTableColumns(Context context) {
        if (databaseType.equals("mysql")) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchMysqlTableColumns(context, jdbcTemplate, map);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchMysqlTableColumns(context, systemJdbcTemplate, map);
            }
            return map;
        }
        if (databaseType.equals("h2")) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchH2TableColumns(context, jdbcTemplate, map);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchH2TableColumns(context, systemJdbcTemplate, map);
            }
            return map;
        }
        throw new ServerErrorException("database type not supported");
    }

    private List<String> fetchMysqlTableList(Context context, JdbcTemplate jdbcTemplate) {

        StopWatch stopWatch = null;
        try {
            String sqlTable = "SHOW TABLES";
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<String> tables = jdbcTemplate.queryForList(sqlTable, String.class);
            if (stopWatch != null) stopWatch.stopNow();
            return tables;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return null;
    }

    private List<String> fetchH2TableList(Context context, JdbcTemplate jdbcTemplate) {

        StopWatch stopWatch = null;
        try {
            String sqlTable = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'";
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<String> tables = jdbcTemplate.queryForList(sqlTable, String.class);
            if (stopWatch != null) stopWatch.stopNow();
            return tables;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return null;
    }

    private boolean fetchMysqlTableColumns(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map) {

        List<String> tables = fetchMysqlTableList(context, jdbcTemplate);
        if (tables == null) {
            return false;
        }

        StopWatch stopWatch = null;
        try {
            for (String table : tables) {
                String sql = "SHOW COLUMNS FROM " + table;
                if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
                List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();
                Map<String, Object> mapPerTable = new LinkedHashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    Map<String, Object> column = columns.get(i);
                    String field = (String) column.get("Field");
                    String type = ((String) column.get("Type")).toLowerCase();
                    String s = (String) column.get("Null");
                    Boolean nullable = false;
                    if (s != null && s.equalsIgnoreCase("yes")) {
                        nullable = true;
                    }
                    String defValue = (String) column.get("Default");
                    String extra = (String) column.get("Extra");
                    if (extra != null && extra.equals("auto_increment")) {
                        defValue = "auto_increment";
                    }
                    Map<String, Object> colMap = new LinkedHashMap<>();
                    colMap.put("type", type);
                    colMap.put("nullable", nullable);
                    colMap.put("default", defValue);
                    mapPerTable.put(field, colMap);
                }
                map.put(table, mapPerTable);
            }
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return false;
    }

    private boolean fetchH2TableColumns(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map) {

        List<String> list = fetchH2TableList(context, jdbcTemplate);

        for (String table: list) {

            String sql = "SELECT TABLE_NAME, COLUMN_NAME, TYPE_NAME, IS_NULLABLE, COLUMN_DEFAULT "+
                    "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'";

            StopWatch stopWatch = null;
            try {
                if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
                List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();

                for (int i = 0; i < columns.size(); i++) {
                    Map<String, Object> column = columns.get(i);
                    String field = (String) column.get("COLUMN_NAME");
                    String type = ((String) column.get("TYPE_NAME")).toLowerCase();
                    String s = (String) column.get("IS_NULLABLE");
                    Boolean nullable = false;
                    if (s != null && s.equalsIgnoreCase("yes")) {
                        nullable = true;
                    }
                    String defValue = (String) column.get("COLUMN_DEFAULT");
                    if (defValue != null && defValue.startsWith("(NEXT VALUE FOR PUBLIC.SYSTEM_SEQUENCE_")) {
                        defValue = "auto_increment";
                    }
                    Map<String, Object> colMap = new LinkedHashMap<>();
                    colMap.put("type", type);
                    colMap.put("nullable", nullable);
                    colMap.put("default", defValue);
                    Map<String, Object> mapPerTable = (Map<String, Object>) map.get(table);
                    if (mapPerTable == null) {
                        mapPerTable = new LinkedHashMap<String, Object>();
                        map.put(table, mapPerTable);
                    }
                    mapPerTable.put(field, colMap);
                }
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    // get auto increment column per table
    //
    private Map<String, Object> fetchTablesAutoIncrementColumns(Context context) {

        if (databaseType.equals("mysql")) {
            Map<String, Object> autoIncMap = new LinkedHashMap<>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchMysqlTableAutoIncrementColumns(context, jdbcTemplate, autoIncMap);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (jdbcTemplate != systemJdbcTemplate) {
                fetchMysqlTableAutoIncrementColumns(context, systemJdbcTemplate, autoIncMap);
            }
            return autoIncMap;
        }
        if (databaseType.equals("h2")) {
            Map<String, Object> autoIncMap = new LinkedHashMap<>();

            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchH2TableAutoIncrementColumns(context, jdbcTemplate, autoIncMap);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (jdbcTemplate != systemJdbcTemplate) {
                fetchH2TableAutoIncrementColumns(context, systemJdbcTemplate, autoIncMap);
            }
            return autoIncMap;
        }
        throw new ServerErrorException("database type not supported");
    }

    // get auto increment column per table from mysql
    //
    private Map<String, Object> fetchMysqlTableAutoIncrementColumns(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> autoIncMap) {

        List<String> tables = fetchMysqlTableList(context, jdbcTemplate);

        for (String table: tables) {

            String sql = "SHOW COLUMNS FROM " + table;

            StopWatch stopWatch = null;
            try {
                if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
                List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();

                if (columns != null) {
                    for (Map<String, Object> column: columns) {
                        String extra = (String) column.get("Extra");
                        if (extra.equals("auto_increment")) {
                            String field = (String) column.get("Field");
                            autoIncMap.put(table, field);
                            break;
                        }
                    }
                } else {
                    throw new ServerErrorException("failed to get columns");
                }
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
        return autoIncMap;
    }

    private boolean fetchH2TableAutoIncrementColumns(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map) {

        String sql = "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_DEFAULT IS NOT NULL";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> columns = jdbcTemplate.queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();
            for (int i = 0; i < columns.size(); i++) {
                Map<String, Object> column = columns.get(i);
                String defaultValue = (String) column.get("COLUMN_DEFAULT");
                if (defaultValue.startsWith("(NEXT VALUE FOR PUBLIC.SYSTEM_SEQUENCE_")) {
                    String table = (String) column.get("TABLE_NAME");
                    String field = (String) column.get("COLUMN_NAME");
                    map.put(table, field);
                }
            }
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return false;
    }

    // get table => unique indexes info
    //
    public Map<String, Object> fetchTableUniqueIndexes(Context context) {
        if (databaseType.equals("mysql")) {
            Map<String, Object> map = new LinkedHashMap<>();
            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchMysqlTableUniqueIndexes(context, jdbcTemplate, map);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchMysqlTableUniqueIndexes(context, systemJdbcTemplate, map);
            }
            return map;
        }
        if (databaseType.equals("h2")) {
            Map<String, Object> map = new LinkedHashMap<>();
            JdbcTemplate jdbcTemplate = AppCtx.getJdbcTemplate();
            fetchH2TableUniqueIndexes(context, jdbcTemplate, map);

            JdbcTemplate systemJdbcTemplate = AppCtx.getSystemJdbcTemplate();
            if (systemJdbcTemplate != jdbcTemplate) {
                fetchH2TableUniqueIndexes(context, systemJdbcTemplate, map);
            }
            return map;
        }
        throw new ServerErrorException("database type not supported");
    }

    private boolean fetchMysqlTableUniqueIndexes(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map) {

        List<String> tables = fetchMysqlTableList(context, jdbcTemplate);

        for (String table: tables) {

            String sql = "SHOW INDEXES FROM " + table + "  WHERE Non_unique = false";

            StopWatch stopWatch = null;
            try {
                if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
                List<Map<String, Object>> indexes = jdbcTemplate.queryForList(sql);
                if (stopWatch != null) stopWatch.stopNow();

                Map<String, Object> mapPerTable = new LinkedHashMap<String, Object>();
                for (int i = 0; i < indexes.size(); i++) {
                    Map<String, Object> index = indexes.get(i);
                    String key_name = (String) index.get("Key_name");
                    List<String> columns = (List<String>) mapPerTable.get(key_name);
                    if (columns == null) {
                        columns = new ArrayList<String>();
                        mapPerTable.put(key_name, columns);
                    }
                    columns.add((String) index.get("Column_name"));
                }
                map.put(table, mapPerTable);
            } catch (Exception e) {
                if (stopWatch != null) stopWatch.stopNow();

                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    // get table unique indexes info from h2
    //
    private boolean fetchH2TableUniqueIndexes(Context context, JdbcTemplate jdbcTemplate, Map<String, Object> map) {

        String sql = "SELECT TABLE_NAME, COLUMN_NAME, PRIMARY_KEY, INDEX_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.INDEXES WHERE NON_UNIQUE = 'FALSE'";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> indexes = jdbcTemplate.queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();

            for (int i = 0; i < indexes.size(); i++) {
                Map<String, Object> index = indexes.get(i);
                String table = (String) index.get("TABLE_NAME");
                String key_name = (String) index.get("INDEX_NAME");
                Map<String, Object> mapPerTable = (Map<String, Object>) map.get(table);
                if (mapPerTable == null) {
                    mapPerTable = new LinkedHashMap<>();
                    map.put(table, mapPerTable);
                }
                List<String> columns = (List<String>) mapPerTable.get(key_name);
                if (columns == null) {
                    columns = new ArrayList<String>();
                    mapPerTable.put(key_name, columns);
                }
                columns.add((String) index.get("COLUMN_NAME"));
            }
            return true;
        } catch (Exception e) {
            if (stopWatch != null) stopWatch.stopNow();

            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            if (context != null) {
                context.logTraceMessage(msg);
            }
            e.printStackTrace();
        }
        return false;
    }
}
