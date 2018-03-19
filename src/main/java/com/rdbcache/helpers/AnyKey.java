/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.models.KeyInfo;

import java.util.ArrayList;
import java.util.List;

public class AnyKey extends ArrayList<KeyInfo> {

    public AnyKey(KeyInfo keyInfo) {
        add(keyInfo);
    }

    public AnyKey() {
    }

    public void setKey(KeyInfo keyInfo) {
        clear();
        add(keyInfo);
    }

    public KeyInfo getKey() {
        if (size() == 0) {
            return null;
        }
        return get(0);
    }

    public KeyInfo getAny() {
        return getAny(0);
    }

    public KeyInfo getAny(int index) {
        if (index > size()) {
            throw new ServerErrorException("getAny index out of range");
        }
        if (size() == 0) {
            KeyInfo keyInfo = new KeyInfo();
            keyInfo.setIsNew(true);
            add(keyInfo);
        } else if (index == size()) {
            KeyInfo keyInfo = get(0).clone();
            keyInfo.setIsNew(true);
            add(keyInfo);
        }
        return get(index);
    }
}
