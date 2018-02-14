/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Utils;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@Entity
@Table(name="rdbcache_kv_pair")
public class KvPair implements Serializable {

    @EmbeddedId
    private KvIdType idType;

    @Transient
    private Map<String, Object> data;

    @Column(insertable = false, updatable = false)
    private Date createdAt;

    @Column(insertable = false, updatable = false)
    private Date updatedAt;

    public KvPair(String id, String type, String value) {
        if (id.equals("*")) {
            throw new ServerErrorException("* id should be replaced by generated id");
        } else {
            idType = new KvIdType(id, type);
        }
        setValue(value);
    }

    public KvPair(String id, String type, Map<String, Object> data) {
        if (id.equals("*")) {
            throw new ServerErrorException("* id should be replaced by generated id");
        } else {
            idType = new KvIdType(id, type);
        }
        this.data = data;
    }

    public KvPair(String id, String type) {
        if (id.equals("*")) {
            throw new ServerErrorException("* id should be replaced by generated id");
        } else {
            idType = new KvIdType(id, type);
        }
        data = new LinkedHashMap<String, Object>();
    }

    public KvPair(String id) {
        if (id.equals("*")) {
            throw new ServerErrorException("* id should be replaced by generated id");
        } else {
            idType = new KvIdType(id);
        }
        data = new LinkedHashMap<String, Object>();
    }

    public KvPair(KvIdType idType) {
        this.idType = idType;
        data = new LinkedHashMap<String, Object>();
    }

    public KvPair() {
        idType = new KvIdType();
        data = new LinkedHashMap<String, Object>();
    }

    public String getId() {
        if (idType == null) return null;
        return idType.getId();
    }

    public void setId(String id) {
        if (idType == null) {
            idType = new KvIdType();
        }
        if (id.equals("*")) {
            throw new ServerErrorException("* id should be replaced by generated id");
        } else {
            idType.setId(id);
        }
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getType() {
        if (idType == null) return null;
        return idType.getType();
    }

    public void setType(String type) {
        if (idType == null) {
            idType = new KvIdType();
        }
        idType.setType(type);
    }

    public KvIdType getIdType() {
        return idType;
    }

    public void setIdType(KvIdType idType) {
        this.idType = idType;
    }

    @Access(AccessType.PROPERTY)
    public String getValue() {
        if (data == null) {
            return null;
        }
        if (data.containsKey("_DEFAULT_")) {
            return (String) data.get("_DEFAULT_");
        } else {
            return Utils.toJson(data);
        }
    }

    public void setValue(String value) {
        Map<String, Object> map = Utils.toMap(value);
        if (map == null) {
            data = new LinkedHashMap<String, Object>();
            int length = value.length();
            if (value.charAt(0) == '"' && value.charAt(length-1) == '"') {
                value = value.substring(1, length - 1);
            }
            data.put("_DEFAULT_", value);
        } else {
            data = filterOutNull(map);
        }
    }

    private Map<String, Object> filterOutNull(Map<String, Object> map) {
        Map<String, Object> newMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry: map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value != null && "null".equals(value)) {
                newMap.put(key, null);
            } else {
                newMap.put(key, value);
            }
        }
        return newMap;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> map) {
        this.data = filterOutNull(map);;
    }

    public Object getMapValue() {
        if (data == null) {
            return null;
        }
        if (data.containsKey("_DEFAULT_")) {
            return data.get("_DEFAULT_");
        } else {
            return data;
        }
    }

    public boolean hasContent() {
        if (data != null && data.size() > 0) {
            return true;
        }
        return false;
    }

    public KvPair clone() {
        return new KvPair(getId(), getType(), getValue());
    }

    @Override
    public String toString() {
        return "KvPair{" +
                "idType='" + idType.toString() + '\'' +
                ", value='" + getValue() + '\'' +
                '}';
    }
}
