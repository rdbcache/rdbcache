/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.rdbcache.models;

import com.rdbcache.helpers.Condition;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class KeyInfo implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyInfo.class);

    private String expire;

    private String table;

    private List<String> indexes;

    private String clause = "";

    private List<Object> params;

    @JsonProperty("query_key")
    private String queryKey = "";

    @JsonIgnore
    private Boolean isNew = true;

    @JsonIgnore
    private Map<String, Object> columns;

    @JsonIgnore
    private Query query;

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

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
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
                ", " + (query == null ? "null" : query.toString()) +
                '}';
    }

    @JsonIgnore
    public Integer getQueryLimit() {
        if (query == null) return null;
        return query.getLimit();
    }

    public boolean readyForQuery(Context context) {
        return readyForQuery(context, false, true);
    }

    public boolean readyForUpdate(Context context) {
        return readyForQuery(context, false, false);
    }

    public boolean readyForDelete(Context context) {
        return readyForQuery(context, false, false);
    }

    public boolean readyForInsert(Context context) {
        return readyForQuery(context, true, false);
    }

    public boolean readyForQuery(Context context, boolean insertOnly, boolean forQuery) {

        if (queryKey == null) return false;
        if (table != null && insertOnly) return true;
        if (table != null && clause != null && params != null) return true;

        KeyInfo keyInfoFound = AppCtx.getKeyInfoRepo().findOne(context);
        if (keyInfoFound != null) {
            copyQueryInfo(keyInfoFound);
            if (queryKey == null) return false;
            if (table == null) return false;
            if (insertOnly) return true;
            if (table != null && clause != null && params != null) return true;
            if (query == null) return false;
        }

        if (insertOnly) return false;

        if (table == null) return false;

        if (forQuery) {
            if (prepareQueryClauseParams(context)) {
                return true;
            } else if (getQueryLimit() != null) {
                return true;
            }
        }

        return preparePKClauseParams(context);
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
        KeyInfo keyInfo = new KeyInfo(expire, table);
        if (indexes != null) keyInfo.indexes = cloneIndexes();
        keyInfo.clause = clause;
        if (params != null) keyInfo.params = cloneParams();
        keyInfo.queryKey = queryKey;
        if (query == null) keyInfo.query = null;
        else keyInfo.query = query.clone();
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

    public List<String> fetchIndexes(Context context) {
        if (indexes != null) {
            return indexes;
        }
        Map<String, Object> map = AppCtx.getDbaseOps().getTableIndexes(context, table);
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
        return indexes;
    }

    public Map<String, Object> fetchColumns(Context context) {
        if (columns != null) {
            return columns;
        }
        columns = AppCtx.getDbaseOps().getTableColumns(context, table);
        return columns;
    }

    @JsonIgnore
    private String stdClause;

    public String fetchStdPKClause(Context context) {
        if (stdClause != null) {
            return stdClause;
        }
        if (clause == null) {
            return null;
        }
        if (indexes == null) {
            fetchIndexes(context);
        }
        if (indexes == null) {
            clause = null;
            return null;
        }
        if (stdClause == null) {
            stdClause = "";
            for (String indexKey : indexes) {
                if (stdClause.length() > 0) {
                    stdClause += " AND ";
                }
                stdClause += indexKey + " = ?";
            }
        }
        return stdClause;
    }

    public boolean hasStdPKClause(Context context) {
        if (clause == null) return false;
        String pkClause = fetchStdPKClause(context);
        if (pkClause == null) return false;
        return pkClause.equals(clause);
    }

    public boolean fetchPKClauseParams(Context context, Map<String, Object> map, String defaultValue) {
        if (clause == null) {
            return false;
        }
        if (indexes == null) {
            fetchIndexes(context);
        }
        if (indexes == null) {
            clause = null;
            return false;
        }
        if (clause.equals(fetchStdPKClause(context))) {
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
            clause = fetchStdPKClause(context);
        }
        if (params == null) {
            params = new ArrayList<Object>();
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

    public boolean preparePKClauseParams(Context context) {
        KvPair pair = context.getPair();
        if (pair == null) {
            return false;
        }
        String key = pair.getId();
        Map<String, Object> map = pair.getData();
        if (map == null) {
            return false;
        }
        return fetchPKClauseParams(context, map, key);
    }

    public boolean prepareQueryClauseParams(Context context) {
        if (query == null || queryKey == null) {
            return false;
        }
        Integer limit = query.getLimit();
        Map<String, Condition> conditions = query.getConditions();
        if (limit == null && (conditions == null || conditions.size() == 0)) {
            queryKey = null;
            query = null;
            return false;
        }
        KvPair pair = context.getPair();
        String key = null;
        if (pair != null) key = pair.getId();
        if (params == null) {
            params = new ArrayList<Object>();
        } else {
            params.clear();
        }
        clause = query.getClause(params, key);
        return (clause != null);
    }
}
