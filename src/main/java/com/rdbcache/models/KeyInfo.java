/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.exceptions.ServerErrorException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.rdbcache.queries.QueryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class KeyInfo implements Serializable, Cloneable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyInfo.class);

    private String expire = Cfg.getDefaultExpire();

    private String table;

    private List<String> indexes;

    private String clause = "";

    private List<Object> params;

    @JsonProperty("query_key")
    private String queryKey = "";

    @JsonIgnore
    private Boolean isNew = false;

    @JsonIgnore
    private Map<String, Object> columns;

    @JsonIgnore
    private QueryInfo query;

    @JsonIgnore
    private String stdClause;

    public KeyInfo(String expire, String table) {
        this.expire = expire;
        this.table = table;
    }

    public KeyInfo(String expire) {
        this.expire = expire;
    }

    public KeyInfo(Map<String, Object> map) {
        fromMap(map);
    }

    public KeyInfo() {
        isNew = true;
    }

    public String getExpire() {
        return expire;
    }

    public void setExpire(String expire) {
        this.expire = expire;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<String> indexes) {
        this.indexes = indexes;
    }

    public List<Object> getParams() {
        return params;
    }

    public String getClause() {
        return clause;
    }

    public void setClause(String clause) {
        this.clause = clause;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    public String getQueryKey() {
        return queryKey;
    }

    public void setQueryKey(String queryKey) {
        this.queryKey = queryKey;
    }

    public Boolean getIsNew() {
        return isNew;
    }

    public void setIsNew(Boolean isNew) {
        this.isNew = isNew;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }

    public QueryInfo getQueryInfo() {
        return query;
    }

    public void setQueryInfo(QueryInfo query) {
        this.query = query;
    }

    public void setStdClause(String stdClause) {
        this.stdClause = stdClause;
    }

    @JsonIgnore
    public String getStdClause() {
        return stdClause;
    }

    @JsonIgnore
    public Long getTTL() {
        Long ttl = Long.valueOf(expire);
        if (ttl < 0l) return -ttl;
        return ttl;
    }

    @JsonIgnore
    public Integer getQueryLimit() {
        if (query == null) return null;
        return query.getLimit();
    }

    public void copyRedisInfo(KeyInfo keyInfo) {
        this.expire = keyInfo.expire;
        this.table = keyInfo.table;
    }

    public void copyQueryInfo(KeyInfo keyInfo) {
        queryKey = keyInfo.queryKey;
        table = keyInfo.table;
        indexes = keyInfo.cloneIndexes();
        clause = keyInfo.clause;
        params = keyInfo.cloneParams();
        if (keyInfo.query != null) {
            query = keyInfo.query.clone();
        } else {
            query = null;
        }
    }

    public KeyInfo clone() {
        KeyInfo keyInfo = null;
        try {
            keyInfo = (KeyInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
        if (indexes != null) keyInfo.indexes = cloneIndexes();
        if (params != null) keyInfo.params = cloneParams();
        if (query != null) keyInfo.query = query.clone();
        return keyInfo;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if (expire != null) map.put("expire", expire);
        if (table != null) map.put("table", table);
        if (indexes != null) map.put("indexes", indexes);
        if (clause != null) map.put("clause", clause);
        if (params != null) map.put("params", params);
        if (queryKey != null) map.put("query_key", queryKey);
        return map;
    }

    public void fromMap(Map<String, Object> map) {
        if (map == null) return;
        if (map.containsKey("expire")) expire = (String) map.get("expire");
        if (map.containsKey("table")) table = (String) map.get("table");
        if (map.containsKey("indexes")) indexes = (List<String>) map.get("indexes");
        if (map.containsKey("clause")) clause = (String) map.get("clause");
        if (map.containsKey("params")) params = (List<Object>) map.get("params");
        if (map.containsKey("query_key")) queryKey = (String) map.get("query_key");
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyInfo keyInfo = (KeyInfo) o;
        return Objects.equals(expire, keyInfo.expire) &&
                Objects.equals(table, keyInfo.table) &&
                Objects.equals(indexes, keyInfo.indexes) &&
                Objects.equals(clause, keyInfo.clause) &&
                Objects.equals(params, keyInfo.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expire, table, indexes, clause, params);
    }

    @Override
    public String toString() {
        return "KeyInfo{" +
                "isNew='" + isNew + '\'' +
                ", table='" + table + '\'' +
                ", expire='" + expire + '\'' +
                (query == null ? "" : ", " + query.toString()) +
                '}';
    }

    private List<String> cloneIndexes() {
        if (indexes == null) {
            return null;
        }
        List<String> newindexes = new ArrayList<String>();
        if (indexes.size() == 0) {
            return newindexes;
        }
        for(String index: indexes) {
            newindexes.add(index);
        }
        return newindexes;
    }

    private List<Object> cloneParams() {
        if (params == null) {
            return null;
        }
        List<Object> newParams = new ArrayList<Object>();
        if (params.size() == 0) {
            return newParams;
        }
        for(Object object: params) {
            newParams.add(object);
        }
        return newParams;
    }
}
