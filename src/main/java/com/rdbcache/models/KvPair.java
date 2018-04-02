/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.rdbcache.helpers.Utils;

import javax.persistence.*;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@Entity
@Table(name="rdbcache_kv_pair")
public class KvPair implements Serializable, Cloneable {

    private static final long serialVersionUID = 20180316L;

    @EmbeddedId
    private KvIdType idType;

    @JsonIgnore
    @Transient
    private Boolean isNewUuid = false;

    @Transient
    private Map<String, Object> data;

    public KvPair(String id, String type, String value) {
        if (id.equals("*")) {
            idType = new KvIdType(Utils.generateId(), type);
            isNewUuid = true;
        } else {
            idType = new KvIdType(id, type);
        }
        setValue(value);
    }

    public KvPair(String id, String type, Map<String, Object> data) {
        if (id.equals("*")) {
            idType = new KvIdType(Utils.generateId(), type);
            isNewUuid = true;
        } else {
            idType = new KvIdType(id, type);
        }
        this.data = data;
    }

    public KvPair(String id, String type) {
        if (id.equals("*")) {
            idType = new KvIdType(Utils.generateId(), type);
            isNewUuid = true;
        } else {
            idType = new KvIdType(id, type);
        }
        data = new LinkedHashMap<String, Object>();
    }

    public KvPair(String id) {
        if (id.equals("*")) {
            idType = new KvIdType(Utils.generateId());
            isNewUuid = true;
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
            idType.setId(Utils.generateId());
            isNewUuid = true;
        } else {
            idType.setId(id);
        }
    }

    @JsonIgnore
    public Boolean isNewUuid() {
        return isNewUuid;
    }

    public void setIsNewUuid(Boolean isNewUuid) {
        this.isNewUuid = isNewUuid;
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

    @JsonIgnore
    public KvIdType getIdType() {
        return idType;
    }

    public void setIdType(KvIdType idType) {
        this.idType = idType;
    }

    @JsonIgnore
    @Access(AccessType.PROPERTY)
    public String getValue() {
        if (data == null) {
            return null;
        }
        if (data.containsKey("_DEFAULT_")) {
            return (String) data.get("_DEFAULT_");
        } else {
            return Utils.toJsonMap(data);
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

    public void setValue(Object value) {
        if (value instanceof Map) {
            setData((Map<String, Object>) value);
        } else {
            setValue(value.toString());
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
        data = filterOutNull(map);;
    }

    public void clearData() {
        data.clear();;
    }

    public String printKey() {
        String key = getId();
        if (key == null) {
            return "null ";
        }
        int length = key.length();
        if (length > 7) {
            key = "..." + key.substring(length - 4);
        }
        key += " ";
        return key;
    }

    public KvPair clone() {
        KvPair clone = new KvPair();
        clone.idType.setId(idType.getId());
        clone.idType.setId(idType.getId());
        clone.isNewUuid = isNewUuid;
        clone.data = getDataClone();
        return clone;
    }

    @JsonIgnore
    public Map<String, Object> getDataClone() {
        if (data == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry: data.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    @JsonIgnore
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

    @Override
    public String toString() {
        return "KvPair{" +
                "idType='" + idType.toString() + '\'' +
                ", value='" + getValue() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KvPair pair = (KvPair) o;

        if (idType != null ? !idType.equals(pair.idType) : pair.idType != null) return false;
        return data != null ? data.equals(pair.data) : pair.data == null;
    }

    @Override
    public int hashCode() {
        int result = idType != null ? idType.hashCode() : 0;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }
}
