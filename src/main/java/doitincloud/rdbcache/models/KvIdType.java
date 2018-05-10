/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.models;

import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

public class KvIdType implements Serializable, Cloneable {

    private static final long serialVersionUID = 20180316L;

    private String id;

    private String type = "data";

    public KvIdType(String id, String type) {
        this.id = id;
        this.type = type;
    }

    public KvIdType(String id) {
        this.id = id;
    }

    public KvIdType(KvIdType idType) {
        id = idType.id;
        type = idType.type;
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

    public KvIdType clone() {
        return new KvIdType(id, type);
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
