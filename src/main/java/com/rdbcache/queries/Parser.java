/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.queries;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Parser {

    private static final Logger LOGGER = LoggerFactory.getLogger(Parser.class);

    private static String[] keyLastCharList = {"!", ">", "<"};

    private static String[] opsList = {
            "_IS_NOT_NULL",
            "_IS_NULL",
            "_IS_NOT_FALSE",
            "_IS_NOT_TRUE",
            "_IS _TRUE",
            "_IS_FALSE",
            "_NOT_LIKE_",
            "_LIKE_",
            "_NOT_REGEXP_",
            "_REGEXP_",
            "_GT_",
            "_GE_",
            "_LT_",
            "_LE_",
            "_EQ_",
            "_NE_",
            ">=",
            "<=",
            "<>",
            ">",
            "<",
            "="
    };

    private static String[] opsTranList = {
            "GT",
            "GE",
            "LT",
            "LE",
            "EQ",
            "NE"
    };

    private static String[] opsTranToList = {
            ">",
            ">=",
            "<",
            "<=",
            "=",
            "!="
    };

    private static String opsOrList[] = { "=" };

    private static String opsSingleList[] = { "IS NOT NULL", "IS NULL", "IS NOT FALSE", "IS NOT TRUE", "IS TRUE", "IS FALSE"};

    public static void setConditions(QueryInfo queryInfo, Map<String, String[]> params) {

        Map<String, Condition> conditions = queryInfo.getConditions();
        if (conditions == null) {
            conditions = new LinkedHashMap<String, Condition>();
            queryInfo.setConditions(conditions);
        }
        for(Map.Entry<String, String[]> entry: params.entrySet()) {

            String key = entry.getKey();
            String[] values = entry.getValue();

            if (key.equalsIgnoreCase("limit")) {
                if (values != null && values.length > 0) {
                    queryInfo.setLimit(Integer.valueOf(values[0]));
                }
                continue;
            }

            String keyLastChar = key.substring(key.length() - 1);
            String ops = null;

            // "!=", ">=", "<="; but "<=>" is not supported
            if (Arrays.asList(keyLastCharList).contains(keyLastChar)) {

                ops = keyLastChar + "=";
                key = key.substring(0, key.length() - 1);

            } else if (values.length == 1 && values[0].length() == 0) {
                for (int i = 0; i < opsList.length; i++) {
                    int index = key.indexOf(opsList[i]);
                    if (index >= 0) {
                        ops = opsList[i];
                        int length = ops.length();
                        String lastOpsChar = ops.substring(ops.length() - 1);
                        ops = ops.replace("_", " ");
                        ops = ops.trim();
                        int pos = Arrays.asList(opsTranList).indexOf(ops);
                        if (pos >= 0) ops = opsTranToList[pos];
                        values[0] = key.substring(index + length);
                        key = key.substring(0, index);
                        if (values[0].startsWith("_") && !lastOpsChar.equals("_")) {
                            values[0] = values[0].substring(1);
                        }
                        break;
                    }
                }
            }
            if (ops == null) {
                ops = "=";
            }
            Condition condition = conditions.get(key);
            if (condition == null) {
                condition = new Condition(ops, values);
                conditions.put(key, condition);
            } else {
                condition.push(ops, values);
            }
        }
    }

    public static String getClause(QueryInfo queryInfo, List<Object> params, String defaultValue) {

        Map<String, Condition> conditions = queryInfo.getConditions();
        if (conditions == null || conditions.size() == 0) {
            return null;
        }
        params.clear();

        String clause = "";
        int total = 0;
        for (Map.Entry<String, Condition> entry: conditions.entrySet()) {
            if (total > 0) {
                clause += " AND ";
            }
            String ckey = entry.getKey();
            Condition condition = entry.getValue();
            int count = 0;
            String exp = "";
            for (Map.Entry<String, List<String>> opsEntry: condition.entrySet()) {
                String ops = opsEntry.getKey();
                if (count > 0) {
                    exp += " AND ";
                }
                List<String> values = opsEntry.getValue();
                String subExp = "";
                if (values == null || values.size() == 0) {
                    subExp += ckey + " = ?";
                    params.add(defaultValue);
                } else {
                    int i = 0;
                    for (; i < values.size(); i++) {
                        String value = values.get(i);
                        if (i > 0) {
                            if (Arrays.asList(opsOrList).contains(ops)) {
                                subExp += " OR ";
                            } else {
                                subExp += " AND ";
                            }
                        }
                        if (Arrays.asList(opsSingleList).contains(ops)) {
                            subExp += ckey + " " + ops;
                        } else {
                            subExp += ckey + " " + ops + " ?";
                            if (value.length() > 0) {
                                params.add(value);
                            } else {
                                params.add(defaultValue);
                            }
                        }
                    }
                }
                exp += "(" + subExp + ")";
                count++;
            }
            if (count > 1) {
                clause += "(" + exp + ")";
            } else {
                clause += exp;
            }
            total++;
        }

        LOGGER.trace("where clause: " + clause + " params: " + params.toString());

        return clause;
    }

    public static boolean prepareQueryClauseParams(Context context, KvPair pair, KeyInfo keyInfo) {
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
        String key = null;
        if (pair != null) key = pair.getId();
        String clause = getClause(queryInfo, params, key);
        keyInfo.setClause(clause);
        return (clause != null);
    }

    public static boolean fetchStdClauseParams(Context context, KeyInfo keyInfo, Map<String, Object> map, String defaultValue) {
        if (keyInfo.getTable() == null || keyInfo.getQueryKey() == null) {
            return false;
        }
        List<String> indexes = keyInfo.getIndexes();
        if (indexes == null) {
            keyInfo.setClause(null);
            keyInfo.setQueryKey(null);
            return false;
        }
        if (keyInfo.getClause() == null) {
            keyInfo.setQueryKey(null);
            return false;
        }
        List<Object> params = keyInfo.getParams();
        if (params != null && params.size() == indexes.size()) {
            String stdClause = "";
            for (String indexKey : indexes) {
                if (stdClause.length() > 0) {
                    stdClause += " AND ";
                }
                stdClause += indexKey + " = ?";
            }
            if (stdClause.equals(keyInfo.getClause())) {
                return true;
            }
        }
        if (params == null) {
            params = new ArrayList<Object>();
            keyInfo.setParams(params);
        } else {
            params.clear();
        }
        String stdClause = "";
        for (String indexKey : indexes) {
            if (stdClause.length() > 0) {
                stdClause += " AND ";
            }
            stdClause += indexKey + " = ?";
            if (map.containsKey(indexKey)) {
                params.add(map.get(indexKey));
            } else if (defaultValue != null) {
                params.add(defaultValue);
            } else {
                return false;
            }
        }
        keyInfo.setClause(stdClause);
        return true;
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
}
