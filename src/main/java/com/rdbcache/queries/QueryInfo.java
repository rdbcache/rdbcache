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

public class QueryInfo implements Serializable {

    private static final long serialVersionUID = 20180316L;

    private String table;

    private Map<String, Condition> conditions = new LinkedHashMap<String, Condition>();

    private Integer limit;

    @JsonIgnore
    private String key;

    public QueryInfo(String table, Map<String, String[]> params) {
        this.table = table;
        Parser.prepareConditions(this, params);
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
            key = DigestUtils.md5Hex(table + toString());
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
        String s2 = "";
        if (conditions != null) {
            s2 = Utils.toJson(conditions).replace("\":", ": ");
            s2 = s2.replace("\"", "");
        }
        String s1 = (limit == null ? "" : "limit: " + limit);
        if (s1.length() > 0) return s1 + ". " + s2;
        else return s2;
    }
}

