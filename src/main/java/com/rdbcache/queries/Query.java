/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.queries;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Query {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    public static boolean readyForQuery(Context context, KeyInfo keyInfo) {
        return readyForQuery(context, keyInfo, false, true);
    }

    public static boolean readyForUpdate(Context context, KeyInfo keyInfo) {
        return readyForQuery(context, keyInfo, false, false);
    }

    public static boolean readyForDelete(Context context, KeyInfo keyInfo) {
        return readyForQuery(context, keyInfo, false, false);
    }

    public static boolean readyForInsert(Context context, KeyInfo keyInfo) {
        return readyForQuery(context, keyInfo, true, false);
    }

    public static boolean readyForQuery(Context context, KeyInfo keyInfo, boolean insertOnly, boolean forQuery) {

        String queryKey = keyInfo.getQueryKey();

        // null means worked on it, conclusion is not for any query
        if (queryKey == null) return false;

        String table = keyInfo.getTable();
        if (table != null && insertOnly) return true;

        String clause = keyInfo.getClause();
        List<Object> params = keyInfo.getParams();
        if (table != null && params != null && clause != null && clause.length() > 0) return true;

        KeyInfo keyFound = new KeyInfo();
        AnyKey anyKeyFound = new AnyKey(keyFound);
        if (AppCtx.getKeyInfoRepo().find(context, anyKeyFound)) {

            keyInfo.copyQueryInfo(keyFound);

            queryKey = keyInfo.getQueryKey();
            if (queryKey == null) return false;

            table = keyInfo.getTable();
            if (table == null) return false;

            if (insertOnly) return true;

            clause = keyInfo.getClause();
            params = keyInfo.getParams();
            if (table != null && params != null && clause != null  && clause.length() > 0) {
                return true;
            }
        }

        if (insertOnly) return false;

        if (table == null) return false;

        if (forQuery) {
            if (prepareQueryClauseParams(context, keyInfo)) {
                return true;
            } else if (keyInfo.getQueryLimit() != null) {
                return true;
            }
        }

        return prepareClauseParams(context, keyInfo);
    }

    public static void save(Context context, QueryInfo queryInfo) {

        KvPair queryPair = new KvPair(queryInfo.getKey(), "query", queryInfo.toMap());
        KvIdType idType = queryPair.getIdType();

        StopWatch stopWatch = context.startStopWatch("dbase", "kvPairRepo.findOne");
        KvPair dbPair = AppCtx.getKvPairRepo().findOne(idType);
        if (stopWatch != null) stopWatch.stopNow();

        if (dbPair != null) {
            if (queryPair.getValue().equals(dbPair.getValue())) {
                return;
            }
        }

        stopWatch = context.startStopWatch("dbase", "kvPairRepo.save");
        AppCtx.getKvPairRepo().save(queryPair);
        if (stopWatch != null) stopWatch.stopNow();
    }

    public static List<String> fetchIndexes(Context context, KeyInfo keyInfo) {
        if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
            return null;
        }
        List<String> indexes = keyInfo.getIndexes();
        if ( indexes != null) {
            return indexes;
        }
        Map<String, Object> map = AppCtx.getDbaseOps().getTableIndexes(context, keyInfo.getTable());
        if (map == null || map.size() == 0) {
            keyInfo.setQueryKey(null);
            return null;
        }
        if (map.containsKey("PRIMARY")) {
            indexes = (List<String>) map.get("PRIMARY");
        } else {
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                indexes = (List<String>) entry.getValue();
                break;
            }
        }
        keyInfo.setIndexes(indexes);
        return indexes;
    }

    public static Map<String, Object> fetchColumns(Context context, KeyInfo keyInfo) {
        if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
            return null;
        }
        Map<String, Object> columns = keyInfo.getColumns();
        if (columns != null) {
            return columns;
        }
        columns = AppCtx.getDbaseOps().getTableColumns(context, keyInfo.getTable());
        keyInfo.setColumns(columns);
        if (columns == null) {
            keyInfo.setQueryKey(null);
        }
        return columns;
    }

    public static boolean fetchClauseParams(Context context, KeyInfo keyInfo, Map<String, Object> map, String defaultValue) {
        if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
            return false;
        }
        String clause = keyInfo.getClause();
        if (clause == null) {
            return false;
        }
        List<String> indexes = fetchIndexes(context, keyInfo);
        if (indexes == null) {
            keyInfo.setClause(null);
            keyInfo.setQueryKey(null);
            return false;
        }
        List<Object> params = keyInfo.getParams();
        if (clause.equals(fetchStdClause(context, keyInfo))) {
            if (params != null && params.size() == indexes.size()) {
                int i = 0;
                for(String indexKey: indexes) {
                    if (map.containsKey(indexKey)) {
                        params.set(i, map.get(indexKey));
                    }
                    i++;
                }
                return true;
            }
        } else {
            clause = fetchStdClause(context, keyInfo);
            keyInfo.setClause(clause);
        }
        if (params == null) {
            params = new ArrayList<Object>();
            keyInfo.setParams(params);
        } else {
            params.clear();
        }
        for(String indexKey: indexes) {
            if (map.containsKey(indexKey)) {
                params.add(map.get(indexKey));
            } else {
                params.add(defaultValue);
            }
        }
        return true;
    }

    public static boolean hasStdClause(Context context, KeyInfo keyInfo) {
        String stdClause = fetchStdClause(context, keyInfo);
        return (stdClause != null);
    }

    public static String fetchStdClause(Context context, KeyInfo keyInfo) {
        String stdClause = keyInfo.getStdClause();
        if (stdClause != null) {
            return stdClause;
        }
        if (keyInfo.getClause() == null) {
            keyInfo.setQueryKey(null);
            return null;
        }
        List<String> indexes = fetchIndexes(context, keyInfo);
        if (indexes == null) {
            keyInfo.setClause(null);
            keyInfo.setQueryKey(null);
            return null;
        }
        stdClause = "";
        for (String indexKey : indexes) {
            if (stdClause.length() > 0) {
                stdClause += " AND ";
            }
            stdClause += indexKey + " = ?";
        }
        keyInfo.setStdClause(stdClause);
        return stdClause;
    }
    
    private static  boolean prepareClauseParams(Context context, KeyInfo keyInfo) {
        KvPair pair = context.getPair();
        if (pair == null) {
            return false;
        }
        String key = pair.getId();
        Map<String, Object> map = pair.getData();
        if (map == null) {
            return false;
        }
        return fetchClauseParams(context, keyInfo, map, key);
    }

    private static boolean prepareQueryClauseParams(Context context, KeyInfo keyInfo) {
        QueryInfo queryInfo = keyInfo.getQueryInfo();
        if (queryInfo == null) {
            return false;
        }
        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }
        Integer limit = queryInfo.getLimit();
        Map<String, Condition> conditions = queryInfo.getConditions();
        if (limit == null && (conditions == null || conditions.size() == 0)) {
            keyInfo.setQueryKey(null);
            keyInfo.setQueryInfo(null);
            return false;
        }
        List<Object> params = keyInfo.getParams();
        if (params == null) {
            params = new ArrayList<Object>();
            keyInfo.setParams(params);
        } else {
            params.clear();
        }
        KvPair pair = context.getPair();
        String key = null;
        if (pair != null) key = pair.getId();
        String clause = Parser.getClause(queryInfo, params, key);
        keyInfo.setClause(clause);
        return (clause != null);
    }
}
