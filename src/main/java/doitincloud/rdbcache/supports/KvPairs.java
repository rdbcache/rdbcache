/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.supports;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KvPairs extends ArrayList<KvPair>{

    public KvPairs(KvIdType idType) {
        add(new KvPair(idType));
    }

    public KvPairs(String id, String value) {
        add(new KvPair(id, "data", value));
    }

    public KvPairs(String id) {
        add(new KvPair(id));
    }

    public KvPairs(List list) {
        for (Object object: list) {
            if (object instanceof  String) {
                String key = (String) object;
                add(new KvPair(key));
            } else if (object instanceof Map) {
                KvPair pair = new KvPair("*");
                pair.setObject(object);
                add(pair);
            }
        }
    }

    public KvPairs(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry: map.entrySet()) {
            KvPair pair = new KvPair(entry.getKey());
            pair.setObject(entry.getValue());
            add(pair);
        }
    }

    public KvPairs(KvPair pair) {
        add(pair);
    }

    public KvPairs() {
    }

    public void setPair(KvPair pair) {
        clear();
        add(pair);
    }

    public KvPair getPair() {
        if (size() == 0) {
            return null;
        }
        return get(0);
    }

    public String getKey() {
        if (size() == 0) {
            return null;
        }
        return get(0).getId();

    }

    public KvPair getAny() {
        return getAny(0);
    }

    public KvPair getAny(int index) {
        if (index > size()) {
            throw new ServerErrorException("getAny index out of range");
        } else if (index == size()) {
            if (index == 0) {
                add(new KvPair("*"));
            } else {
                String type = get(0).getType();
                add(new KvPair("*", type));
            }
        }
        return get(index);
    }

    public List<String> getKeys() {
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < size(); i++) {
            keys.add(get(i).getId());
        }
        return keys;
    }

    public String printKey() {
        int size = size();
        if (size == 0) {
            return "null";
        }
        String key = get(0).printKey();
        if (size > 1) {
            key += "... ";
        }
        return key;
    }

    public KvPairs clone() {
        KvPairs clone = new KvPairs();
        for (KvPair pair: this) {
            clone.add(pair.clone());
        }
        return clone;
    }
}
