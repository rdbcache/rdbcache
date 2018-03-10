/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryCheck {

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
        if (queryKey == null) return false;
        String table = keyInfo.getTable();
        if (table != null && insertOnly) return true;
        String clause = keyInfo.getClause();
        List<Object> params = keyInfo.getParams();
        if (table != null && clause != null && params != null) return true;

        KeyInfo keyInfoFound = AppCtx.getKeyInfoRepo().findOne(context);
        if (keyInfoFound != null) {
            keyInfo.copyQueryInfo(keyInfoFound);
            queryKey = keyInfo.getQueryKey();
            if (queryKey == null) return false;
            table = keyInfo.getTable();
            if (table == null) return false;
            if (insertOnly) return true;
            clause = keyInfo.getClause();
            params = keyInfo.getParams();
            if (table != null && clause != null && params != null) return true;
            if (keyInfo.getQuery() == null) return false;
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
        return preparePKClauseParams(context, keyInfo);
    }

    public static List<String> fetchIndexes(Context context, KeyInfo keyInfo) {
        List<String> indexes = keyInfo.getIndexes();
        if ( indexes != null) {
            return indexes;
        }
        Map<String, Object> map = AppCtx.getDbaseOps().getTableIndexes(context, keyInfo.getTable());
        if (map == null || map.size() == 0) {
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
        Map<String, Object> columns = keyInfo.getColumns();
        if (columns != null) {
            return columns;
        }
        columns = AppCtx.getDbaseOps().getTableColumns(context, keyInfo.getTable());
        keyInfo.setColumns(columns);
        return columns;
    }

    public static boolean fetchPKClauseParams(Context context, KeyInfo keyInfo, Map<String, Object> map, String defaultValue) {
        String clause = keyInfo.getClause();
        if (clause == null) {
            return false;
        }
        List<String> indexes = keyInfo.getIndexes();
        if (indexes == null) {
            fetchIndexes(context, keyInfo);
        }
        indexes = keyInfo.getIndexes();
        if (indexes == null) {
            keyInfo.setClause(null);
            return false;
        }
        List<Object> params = keyInfo.getParams();
        if (clause.equals(keyInfo.getStdClause(context))) {
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
            clause = keyInfo.getStdClause(context);
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

    public static String fetchStdClause(Context context, KeyInfo keyInfo) {
        if (keyInfo.getClause() == null) {
            return null;
        }
        List<String> indexes = keyInfo.getIndexes();
        if (indexes == null) {
            fetchIndexes(context, keyInfo);
        }
        indexes = keyInfo.getIndexes();
        if (indexes == null) {
            keyInfo.setClause(null);
            return null;
        }
        String stdClause = "";
        for (String indexKey : indexes) {
            if (stdClause.length() > 0) {
                stdClause += " AND ";
            }
            stdClause += indexKey + " = ?";
        }
        return stdClause;
    }
    
    public static  boolean preparePKClauseParams(Context context, KeyInfo keyInfo) {
        KvPair pair = context.getPair();
        if (pair == null) {
            return false;
        }
        String key = pair.getId();
        Map<String, Object> map = pair.getData();
        if (map == null) {
            return false;
        }
        return fetchPKClauseParams(context, keyInfo, map, key);
    }

    public static boolean prepareQueryClauseParams(Context context, KeyInfo keyInfo) {
        Query query = keyInfo.getQuery();
        if (query == null) {
            return false;
        }
        String queryKey = keyInfo.getQueryKey();
        if (queryKey == null) {
            return false;
        }
        Integer limit = query.getLimit();
        Map<String, Condition> conditions = query.getConditions();
        if (limit == null && (conditions == null || conditions.size() == 0)) {
            keyInfo.setQueryKey(null);
            keyInfo.setQuery(null);
            return false;
        }
        KvPair pair = context.getPair();
        String key = null;
        if (pair != null) key = pair.getId();
        List<Object> params = keyInfo.getParams();
        if (params == null) {
            params = new ArrayList<Object>();
            keyInfo.setParams(params);
        } else {
            params.clear();
        }
        String clause = QueryBuilder.getClause(query, params, key);
        keyInfo.setClause(clause);
        return (clause != null);
    }
}
