/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.queries;

import com.rdbcache.exceptions.ServerErrorException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.rdbcache.helpers.Utils;
import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class QueryInfo implements Serializable, Cloneable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInfo.class);

    private String table;

    private Map<String, Condition> conditions = new LinkedHashMap<String, Condition>();

    private Integer limit;

    @JsonIgnore
    private String key;

    public QueryInfo(String table, Map<String, String[]> params) {
        this.table = table;
        Parser.setConditions(this, params);
    }

    public QueryInfo(String table) {
        this.table = table;
    }

    public QueryInfo(Map<String, Object> map) {
        fromMap(map);
    }

    public QueryInfo() {
    }

    public String getKey() {
        if (key == null) {
            key = DigestUtils.md5Hex(table + Utils.toJson(conditions) + (limit == null ? "" : limit.toString()));
        }
        return key;
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

    @Override
    public String toString() {
        return (limit == null ? "" : "limit=" + limit + ",") +
                (conditions == null ? "" : Utils.toJson(conditions));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryInfo queryInfo = (QueryInfo) o;

        if (table != null ? !table.equals(queryInfo.table) : queryInfo.table != null) return false;
        if (limit != null ? !limit.equals(queryInfo.limit) : queryInfo.limit != null) return false;
        return isConditionsEqual(conditions, queryInfo.conditions);
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (conditions != null ? conditions.hashCode() : 0);
        return result;
    }

    public QueryInfo clone() {
        QueryInfo queryInfo = null; //new QueryInfo(table);
        try {
            queryInfo = (QueryInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
        if (conditions != null) queryInfo.conditions = cloneConditions();
        return queryInfo;
    }

    private Map<String, Condition> cloneConditions() {
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

    private boolean isConditionsEqual(Map<String, Condition> a, Map<String, Condition> b) {
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
}

