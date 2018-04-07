/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.helpers.Context;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.commons.helpers.Utils;
import doitincloud.commons.helpers.VersionInfo;
import doitincloud.rdbcache.models.*;
import doitincloud.rdbcache.queries.QueryInfo;
import doitincloud.rdbcache.repositories.KvPairRepo;
import doitincloud.rdbcache.repositories.MonitorRepo;
import doitincloud.rdbcache.repositories.StopWatchRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class DbaseOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbaseOps.class);

    private Long tableInfoCacheTTL = PropCfg.getTableInfoCacheTTL();

    private String databaseType = "h2";

    @Autowired
    JdbcTemplate jdbcTemplate;

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

    public String formatDate(String type, Date date) {
        if (type.startsWith("year") || type.equals("year")) {
            SimpleDateFormat sdf = Utils.getYearFormat();
            return sdf.format(date);
        } else if (type.equals("time")) {
            SimpleDateFormat sdf = Utils.getTimeFormat();
            return sdf.format(date);
        } else if (type.equals("date")) {
            SimpleDateFormat sdf = Utils.getDateFormat();
            return sdf.format(date);
        } else if (type.equals("datetime") || type.equals("timestamp")) {
            SimpleDateFormat sdf = Utils.getDateTimeFormat();
            return sdf.format(date);
        }
        assert false : "not supported type " + type;
        return null;
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
                    dbMap.put(key, formatDate(type, dateValue));
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
        Optional<KvPair> optional = AppCtx.getKvPairRepo().findById(idType);
        KvPair pair = null;
        if (!optional.isPresent()) {
            pair = new KvPair(idType);
        } else {
            pair = optional.get();
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

        StopWatchRepo stopWatchRepo = AppCtx.getStopWatchRepo();
        if (stopWatchRepo == null) {
            return false;
        }

        List<StopWatch> watches = monitor.getStopWatches();
        if (watches != null && watches.size() > 0) {
            for (StopWatch watch: watches) {
                watch.setMonitorId(monitor.getId());
                stopWatchRepo.save(watch);
            }
        }
        return true;
    }

    // save query info to database
    //
    public static boolean saveQuery(Context context, QueryInfo queryInfo) {

        KvPairRepo kvPairRepo = AppCtx.getKvPairRepo();
        if (kvPairRepo == null) {
            return false;
        }

        KvPair queryPair = new KvPair(queryInfo.getKey(), "query", Utils.toMap(queryInfo));
        KvIdType idType = queryPair.getIdType();

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        Optional<KvPair> optional = kvPairRepo.findById(idType);
        if (stopWatch != null) stopWatch.stopNow();

        if (optional.isPresent()) {
            KvPair dbPair = optional.get();
            if (queryPair.getData().equals(dbPair.getData())) {
                return true;
            }
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        kvPairRepo.save(queryPair);
        if (stopWatch != null) stopWatch.stopNow();

        return true;
    }

    public List<String> getTableList(Context context) {

        Map<String, Object> tablesMap = (Map<String, Object>) AppCtx.getCacheOps().get("table_list");
        if (tablesMap != null) {
            return (List<String>) tablesMap.get("tables");
        }

        tablesMap = (Map<String, Object>) AppCtx.getCacheOps().put("table_list", tableInfoCacheTTL * 1000L, () -> {
            List<String> alltables = fetchTableList(context);
            if (alltables == null) {
                LOGGER.error("failed to get table list");
                return null;
            }
            List<String> tables = new ArrayList<String>();
            for (String table : alltables) {
                Map<String, Object> indexes = getTableIndexes(context, table);
                if (indexes != null  && indexes.size() > 0) {
                    tables.add(table);
                } else {
                    LOGGER.warn("skip " + table + " table, due to no primary or unique index info");
                }
            }
            Map<String, Object> map = new HashMap<>();
            map.put("tables", tables);
            return map;
        });
        if (tablesMap == null) {
            throw new ServerErrorException("failed to get table list");
        }

        return (List<String>) tablesMap.get("tables");
    }

    public Map<String, Object> getTableColumns(Context context, String table) {

        Map<String, Object> columns = (Map<String, Object>) AppCtx.getCacheOps().get("table_columns::" + table);
        if (columns != null) {
            return columns;
        }

        columns = (Map<String, Object>) AppCtx.getCacheOps().put("table_columns::" + table, tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> map = fetchTableColumns(context, table);
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

        return columns;
    }

    public String getTableAutoIncColumn(Context context, String table) {

        Map<String, Object> columns = (Map<String, Object>) AppCtx.getCacheOps().get("table_auto_inc_columns");
        if (columns != null) {
            if (table == null) return null;
            return (String) columns.get(table);
        }

        columns = (Map<String, Object>) AppCtx.getCacheOps().put("table_auto_inc_columns", tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> map = fetchTableAutoIncrementColumns(context);
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

        Map<String, Object> map = (Map<String, Object>) AppCtx.getCacheOps().get("table_indexes::" + table);
        if (map != null) {
            return map;
        }

        Object object = AppCtx.getCacheOps().put("table_indexes::" + table, tableInfoCacheTTL * 1000L, () -> {
            Map<String, Object> indexes = fetchTableUniqueIndexes(context, table);
            if (indexes == null) {
                String msg = "failed to get table indexes";
                LOGGER.error(msg);
                if (context != null) {
                    context.logTraceMessage(msg);
                }
            }
            return indexes;
        });
        if (object == null) {
            throw new ServerErrorException("failed to get table indexes");
        }

        map = (Map<String, Object>) object;

        return map;
    }

    public void cacheAllTablesInfo() {

        List<String> tables = getTableList(null);
        for (String table: tables) {
            getTableColumns(null, table);
            getTableIndexes(null, table);
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

    // get list of tables from mysql
    //
    public List<String> fetchTableList(Context context) {
        if (databaseType.equals("mysql")) {
            return fetchMysqlTableList(context);
        }
        if (databaseType.equals("h2")) {
            return fetchH2TableList(context);
        }
        throw new ServerErrorException("database type not supported");
    }

    // get list of tables from mysql
    //
    private List<String> fetchMysqlTableList(Context context) {

        String sql = "SHOW TABLES";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<String> tables = AppCtx.getJdbcTemplate().queryForList(sql, String.class);
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
        throw new ServerErrorException("failed to get database table list");
    }

    // get list of tables from h2
    //
    private List<String> fetchH2TableList(Context context) {

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";

        List<String> tables = new ArrayList<>();

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> list = AppCtx.getJdbcTemplate().queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();

            for (Map<String, Object> map: list) {
                if ("TABLE".equals(map.get("TABLE_TYPE"))) {
                    tables.add((String) map.get("TABLE_NAME"));
                }
            }

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
        throw new ServerErrorException("failed to get database table list");
    }

    // get table columns info
    //
    public Map<String, Object> fetchTableColumns(Context context, String table) {
        if (databaseType.equals("mysql")) {
            return fetchMysqlTableColumns(context, table);
        }
        if (databaseType.equals("h2")) {
            return fetchH2TableColumns(context, table);
        }
        throw new ServerErrorException("database type not supported");
    }

    // get table columns info from mysql
    //
    private Map<String, Object> fetchMysqlTableColumns(Context context, String table) {

        Map<String, Object> map = new LinkedHashMap<String, Object>();

        String sql = "SHOW COLUMNS FROM " + table;

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> columns = AppCtx.getJdbcTemplate().queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();
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
                map.put(field, colMap);
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

        throw new ServerErrorException("failed to get database table columns");
    }

    // get table columns info from h2
    //
    public Map<String, Object> fetchH2TableColumns(Context context, String table) {

        Map<String, Object> map = new LinkedHashMap<String, Object>();

        String sql = "SELECT COLUMN_NAME, TYPE_NAME, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + table + "'";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> columns = AppCtx.getJdbcTemplate().queryForList(sql);
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
                map.put(field, colMap);
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
        return map;
    }

    // get auto increment column per table
    //
    private Map<String, Object> fetchTableAutoIncrementColumns(Context context) {

        if (databaseType.equals("mysql")) {
            return fetchMysqlTableAutoIncrementColumns(context);
        }
        if (databaseType.equals("h2")) {
            return fetchH2TableAutoIncrementColumns(context);
        }
        throw new ServerErrorException("database type not supported");
    }

    // get auto increment column per table from mysql
    //
    private Map<String, Object> fetchMysqlTableAutoIncrementColumns(Context context) {

        Map<String, Object> autoIncMap = new LinkedHashMap<>();

        List<String> tables = getTableList(context);
        for (String table: tables) {

            String sql = "SHOW COLUMNS FROM " + table;

            StopWatch stopWatch = null;
            try {
                if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
                List<Map<String, Object>> columns = AppCtx.getJdbcTemplate().queryForList(sql);
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

    // get auto increment column per table from h2
    //
    public Map<String, Object> fetchH2TableAutoIncrementColumns(Context context) {

        Map<String, Object> autoIncMap = new LinkedHashMap<>();

        String sql = "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_DEFAULT IS NOT NULL";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> columns = AppCtx.getJdbcTemplate().queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();
            for (int i = 0; i < columns.size(); i++) {
                Map<String, Object> column = columns.get(i);
                String defaultValue = (String) column.get("COLUMN_DEFAULT");
                if (defaultValue.startsWith("(NEXT VALUE FOR PUBLIC.SYSTEM_SEQUENCE_")) {
                    String table = (String) column.get("TABLE_NAME");
                    String field = (String) column.get("COLUMN_NAME");
                    autoIncMap.put(table, field);
                }
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
        return autoIncMap;
    }

    // get table unique indexes info
    //
    public Map<String, Object> fetchTableUniqueIndexes(Context context, String table) {
        if (databaseType.equals("mysql")) {
            return fetchMysqlTableUniqueIndexes(context, table);
        }
        if (databaseType.equals("h2")) {
            return fetchH2TableUniqueIndexes(context, table);
        }
        throw new ServerErrorException("database type not supported");
    }

    // get table unique indexes info from mysql
    //
    private Map<String, Object> fetchMysqlTableUniqueIndexes(Context context, String table) {

        String sql = "SHOW INDEXES FROM " + table + "  WHERE Non_unique = false";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
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

        throw new ServerErrorException("failed to get database table indexes");
    }

    // get table unique indexes info from h2
    //
    private Map<String, Object> fetchH2TableUniqueIndexes(Context context, String table) {

        String sql = "SELECT TABLE_NAME, COLUMN_NAME, PRIMARY_KEY, INDEX_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.INDEXES WHERE NON_UNIQUE = 'FALSE' AND TABLE_NAME = '" + table + "'";

        StopWatch stopWatch = null;
        try {
            if (context != null) stopWatch = context.startStopWatch("dbase", "jdbcTemplate.queryForList");
            List<Map<String, Object>> indexes = AppCtx.getJdbcTemplate().queryForList(sql);
            if (stopWatch != null) stopWatch.stopNow();

            Map<String, Object> map = new LinkedHashMap<String, Object>();
            for (int i = 0; i < indexes.size(); i++) {
                Map<String, Object> index = indexes.get(i);
                String key_name = (String) index.get("INDEX_NAME");
                List<String> columns = (List<String>) map.get(key_name);
                if (columns == null) {
                    columns = new ArrayList<String>();
                    map.put(key_name, columns);
                }
                columns.add((String) index.get("COLUMN_NAME"));
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

        throw new ServerErrorException("failed to get database table indexes");
    }
}
