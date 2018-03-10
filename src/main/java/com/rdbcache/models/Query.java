/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class Query implements Serializable, Cloneable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    private String table;

    private Map<String, Condition> conditions = new LinkedHashMap<String, Condition>();

    private Integer limit;

    @JsonIgnore
    private String key;

    public Query(String table, Map<String, String[]> params) {
        this.table = table;
        QueryBuilder.setConditionsFromParams(this, params);
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
        return QueryBuilder.isConditionsEqual(conditions, query.conditions);
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
        Query query = null; //new Query(table);
        try {
            query = (Query) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
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
}
