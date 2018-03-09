/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.models;

import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class KvIdType implements Serializable {

    private String id;

    private String type;

    public KvIdType(String id, String type) {
        this.id = id;
        this.type = type;
    }

    public KvIdType(String id) {
        this.id = id;
        this.type = "data";
    }

    public KvIdType() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KvIdType kvIdType = (KvIdType) o;
        return Objects.equals(id, kvIdType.id) &&
                Objects.equals(type, kvIdType.type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return "{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
