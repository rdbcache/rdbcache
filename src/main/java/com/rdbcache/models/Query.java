/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.rdbcache.helpers.Condition;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Utils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class Query implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    private String table;

    private Map<String, Condition> conditions = new LinkedHashMap<String, Condition>();

    private Integer limit;

    @JsonIgnore
    private String key;

    public Query(String table, Map<String, String[]> params) {
        this.table = table;
        setConditionsFromParams(params);
    }

    public Query(String table) {
        this.table = table;
    }

    public Query(Map<String, Object> map) {
        fromMap(map);
    }

    public Query() {
    }

    public String getKey() {
        if (key == null) {
            key = DigestUtils.md5Hex(table + Utils.toJson(conditions) + (limit == null ? "" : limit.toString()));
        }
        return key;
    }

    public String getClause(List<Object> params, String defaultValue) {
        return generateClauseAndParams(params, defaultValue);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Condition> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, Condition> conditions) {
        this.conditions = conditions;
    }

    public void setConditions(String key, String value) {
        conditions.put(key, new Condition(value));
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {
        return "Query{" +
                "limit=" + limit +
                ", conditions=" + (conditions == null ? "null" : Utils.toJson(conditions)) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        if (table != null ? !table.equals(query.table) : query.table != null) return false;
        if (limit != null ? !limit.equals(query.limit) : query.limit != null) return false;
        return isConditionsEqual(conditions, query.conditions);
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (conditions != null ? conditions.hashCode() : 0);
        return result;
    }

    public Query clone() {
        Query query = new Query(table);
        query.limit = limit;
        if (conditions != null) query.conditions = cloneConditions();
        return query;
    }

    public Map<String, Condition> cloneConditions() {
        if (conditions == null) {
            return null;
        }
        Map<String, Condition> cmap = new LinkedHashMap<String, Condition>();
        if (conditions.size() == 0) {
            return cmap;
        }
        for(Map.Entry<String, Condition> entry: conditions.entrySet()) {
            Condition condition = entry.getValue();
            cmap.put(entry.getKey(), condition.clone());
        }
        return cmap;
    }


    public void fromMap(Map<String, Object> map) {
        if (map.containsKey("table")) table = (String) map.get("table");
        if (map.containsKey("limit")) limit = Integer.valueOf(map.get("limit").toString());
        if (map.containsKey("conditions")) {
            Map<String, Object> cmap = (Map<String, Object>) map.get("conditions");
            if (conditions == null) conditions = new LinkedHashMap<>();
            for(Map.Entry<String, Object> entry: cmap.entrySet()) {
                conditions.put(entry.getKey(), new Condition((Map<String, Object>) entry.getValue()));
            }
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if (table != null) map.put("table", table);
        if (limit != null) map.put("limit", limit);
        if (conditions != null) {
            Map<String, Object> cmap = new LinkedHashMap<String, Object>();
            map.put("conditions", cmap);
            for (Map.Entry<String, Condition> entry: conditions.entrySet()) {
                Condition condition = entry.getValue();
                Map<String, Object> tmap = condition.toMap();
                cmap.put(entry.getKey(), tmap);
            }
        }
        return map;
    }

    public void save(Context context) {

        KvPair queryPair = new KvPair(getKey(), "query", toMap());
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

    public static boolean isConditionsEqual(Map<String, Condition> a, Map<String, Condition> b) {
        if (a == b) {
            return true;
        }
        if ((a == null || a.size() == 0) && (b == null || b.size() == 0)) {
            return true;
        }
        if ((a == null || a.size() == 0) || (b == null || b.size() == 0)) {
            return false;
        }
        if (a.size() != b.size()) {
            return false;
        }
        boolean isSame = true;
        for (Map.Entry<String, Condition> entry: a.entrySet()) {
            if (!b.containsKey(entry.getKey())) {
                isSame = false;
                break;
            }
            Condition ac = entry.getValue();
            Condition bc = b.get(entry.getKey());
            if (ac == bc) {
                continue;
            } else if (ac == null || bc == null) {
                isSame = false;
                break;
            }
            if (!ac.equals(bc)) {
                isSame = false;
                break;
            }
        }
        return isSame;
    }

    static String[] keyLastCharList = {"!", ">", "<"};

    static String[] opsList = {
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

    static String[] opsTranList = {
            "GT",
            "GE",
            "LT",
            "LE",
            "EQ",
            "NE"
    };

    static String[] opsTranToList = {
            ">",
            ">=",
            "<",
            "<=",
            "=",
            "!="
    };

    public void setConditionsFromParams(Map<String, String[]> params) {

        if (conditions == null) {
            conditions = new LinkedHashMap<String, Condition>();
        }
        for(Map.Entry<String, String[]> entry: params.entrySet()) {

            String key = entry.getKey();
            String[] values = entry.getValue();

            if (key.equalsIgnoreCase("limit")) {
                if (values != null && values.length > 0) {
                    limit = Integer.valueOf(values[0]);
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

    private static String opsOrList[] = { "=" };

    private static String opsSingleList[] = { "IS NOT NULL", "IS NULL", "IS NOT FALSE", "IS NOT TRUE", "IS TRUE", "IS FALSE"};

    private String generateClauseAndParams(List<Object> params, String defaultValue) {

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
            Map<String, List<String>> map = condition.getMap();
            int count = 0;
            String exp = "";
            for (Map.Entry<String, List<String>> opsEntry: map.entrySet()) {
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

        LOGGER.trace("where clause: " + clause);
        LOGGER.trace("where params: " + Utils.toJson(params));

        return clause;
    }
}
