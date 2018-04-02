/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.helpers;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.models.KeyInfo;

import java.util.ArrayList;

public class AnyKey extends ArrayList<KeyInfo> {

    public AnyKey(KeyInfo keyInfo) {
        add(keyInfo);
    }

    public AnyKey() {
    }

    public void setKeyInfo(KeyInfo keyInfo) {
        clear();
        add(keyInfo);
    }

    public KeyInfo getKeyInfo() {
        if (size() == 0) {
            return null;
        }
        return get(0);
    }

    public KeyInfo getAny() {
        return getAny(0);
    }

    public KeyInfo getAny(int index) {
        int size = size();
        if (index > size) {
            throw new ServerErrorException("getAny index out of range");
        }
        if (size == 0 || index == size) {
            KeyInfo keyInfo = null;
            if (size == 0) {
                keyInfo = new KeyInfo();
                keyInfo.setIsNew(true);
            } else {
                keyInfo = get(0).clone();
                keyInfo.setIsNew(true);
                keyInfo.clearParams();
            }
            add(keyInfo);
        }
        return get(index);
    }

    public String printTable() {
        if (size() == 0) {
            return null;
        }
        String s = get(0).getTable();
        if (size() > 1) {
            s += "...";
        }
        return s;
    }

    public String print() {
        if (size() == 0) {
            return null;
        }
        String s = get(0).toString();
        if (size() > 1) {
            s += "...";
        }
        return s;
    }

    public AnyKey clone() {
        AnyKey anyKey = new AnyKey();
        for (KeyInfo keyInfo: this) {
            anyKey.add(keyInfo.clone());
        }
        return anyKey;
    }
}
