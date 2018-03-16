/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.exceptions.ServerErrorException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.rdbcache.queries.QueryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class KeyInfo implements Serializable, Cloneable {

    private static final long serialVersionUID = 20180316L;

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyInfo.class);

    private String expire = PropCfg.getDefaultExpire();

    private String table;

    private String clause = "";

    private List<Object> params;

    @JsonProperty("query_key")
    private String queryKey = "";  // null means worked on it, conclusion is not for any query

    @JsonIgnore
    private Boolean isNew = false;

    @JsonIgnore
    private List<String> indexes;

    @JsonIgnore
    private Map<String, Object> columns;

    @JsonIgnore
    private QueryInfo query;

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

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    public void clearParams() {
        if (params != null) params.clear();
    }

    public String getClause() {
        return clause;
    }

    public void setClause(String clause) {
        this.clause = clause;
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

    public List<String> getIndexes() {
        if (indexes != null) {
            return indexes;
        }
        if (table == null || queryKey == null) {
            return null;
        }
        Map<String, Object> map = AppCtx.getDbaseOps().getTableIndexes(null, table);
        if (map == null || map.size() == 0) {
            queryKey = null;
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
        return indexes;
    }

    public void setIndexes(List<String> indexes) {
        this.indexes = indexes;
    }

    public Map<String, Object> getColumns() {
        if (columns != null) {
            return columns;
        }
        if (table == null || queryKey == null) {
            return null;
        }
        columns = AppCtx.getDbaseOps().getTableColumns(null, table);
        if (columns == null) {
            queryKey = null;
        }
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }

    public QueryInfo getQueryInfo() {
        return query;
    }

    public void setQueryInfo(QueryInfo query) {
        if (query != null) {
            queryKey = query.getKey();
        }
        this.query = query;
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

    public KeyInfo clone() {
        KeyInfo keyInfo = null;
        try {
            keyInfo = (KeyInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
        if (params != null) {
            keyInfo.params = new ArrayList<>();
            for (Object p: params) {
                keyInfo.params.add(p);
            }
        }
        return keyInfo;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        if (expire != null) map.put("expire", expire);
        if (table != null) map.put("table", table);
        if (clause != null) map.put("clause", clause);
        if (params != null) {
            List<Object> ps = new ArrayList<>();
            for (Object p: params) {
                ps.add(p);
            }
            map.put("params", ps);
        }
        if (queryKey != null) map.put("query_key", queryKey);
        return map;
    }

    public void fromMap(Map<String, Object> map) {
        if (map == null) return;
        if (map.containsKey("expire")) expire = (String) map.get("expire");
        if (map.containsKey("table")) table = (String) map.get("table");
        if (map.containsKey("clause")) clause = (String) map.get("clause");
        if (map.containsKey("params")) {
            List<Object> ps = (List<Object>) map.get("params");
            params = new ArrayList<>();
            for (Object p: ps) {
                params.add(p);
            }
        }
        if (map.containsKey("query_key")) queryKey = (String) map.get("query_key");
        else queryKey = null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyInfo keyInfo = (KeyInfo) o;
        return Objects.equals(expire, keyInfo.expire) &&
                Objects.equals(table, keyInfo.table) &&
                Objects.equals(clause, keyInfo.clause) &&
                Objects.equals(params, keyInfo.params) &&
                Objects.equals(queryKey, keyInfo.queryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expire, table, indexes, clause, params, queryKey);
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
}
