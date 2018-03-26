/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.rdbcache.configs.AppCtx;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.exceptions.ServerErrorException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.rdbcache.queries.QueryInfo;

import java.io.Serializable;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KeyInfo implements Cloneable {

    private String expire = PropCfg.getDefaultExpire();

    private String table;

    private String clause = "";

    private List<Object> params;

    @JsonProperty("query_key")
    private String queryKey = "";  // null means worked on it, conclusion is not for any query

    @JsonIgnore
    private Boolean isNew = false;

    @JsonIgnore
    private String expireOld;

    @JsonIgnore
    private QueryInfo query;

    @JsonIgnore
    private List<String> primaryIndexes;

    @JsonIgnore
    private Map<String, Object> columns;

    public KeyInfo() {
    }

    public String getExpire() {
        return expire;
    }

    public void setExpire(String expire) {
        if (!this.expire.equals(expire)) {
            expireOld = this.expire;
            this.expire = expire;
        }
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

    public QueryInfo getQuery() {
        return query;
    }

    public void setQuery(QueryInfo query) {
        if (query != null) {
            queryKey = query.getKey();
        }
        this.query = query;
    }

    public String getExpireOld() {
        return expireOld;
    }

    public void restoreExpire() {
        if (expireOld != null) {
            expire = expireOld;
        }
    }

    @JsonIgnore
    public List<String> getPrimaryIndexes() {
        if (primaryIndexes != null) {
            return primaryIndexes;
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
            primaryIndexes = (List<String>) map.get("PRIMARY");
        } else {
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                primaryIndexes = (List<String>) entry.getValue();
                break;
            }
        }
        return primaryIndexes;
    }

    public void setPrimaryIndexes(List<String> indexes) {
        primaryIndexes = indexes;
    }

    @JsonIgnore
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

    @JsonIgnore
    public Long getExpireTTL() {
        Long ttl = Long.valueOf(expire);
        if (ttl < 0l) return -ttl;
        return ttl;
    }

    @JsonIgnore
    public Integer getQueryLimit() {
        if (query == null) return null;
        return query.getLimit();
    }

    public void cleanup() {
        expireOld = null;
        if (query != null && queryKey != null && queryKey.length() == 0) {
            queryKey = query.getKey();
            query = null;
        }
        columns = null;
        primaryIndexes = null;
        isNew = false;
    }

    @Override
    public KeyInfo clone() {
        try {
            KeyInfo keyInfo = (KeyInfo) super.clone();
            if (params != null) {
                keyInfo.params = new ArrayList<>();
                for (Object p: params) {
                    keyInfo.params.add(p);
                }
            }
            return keyInfo;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyInfo keyInfo = (KeyInfo) o;

        if (expire != null ? !expire.equals(keyInfo.expire) : keyInfo.expire != null) return false;
        if (table != null ? !table.equals(keyInfo.table) : keyInfo.table != null) return false;
        if (clause != null ? !clause.equals(keyInfo.clause) : keyInfo.clause != null) return false;
        if (params != null ? !params.equals(keyInfo.params) : keyInfo.params != null) return false;
        if (queryKey != null ? !queryKey.equals(keyInfo.queryKey) : keyInfo.queryKey != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = expire != null ? expire.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (clause != null ? clause.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        result = 31 * result + (queryKey != null ? queryKey.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "KeyInfo(" +
                isNew +
                ", " + table +
                ", " + expire +
                (clause != null && clause.length() > 0 ? ", " + clause : "") +
                (params != null && params.size() > 0 ? ", " + params.toString() : "") +
                (query == null ? "" : ", " + query.toString()) +
                ')';
    }
}
