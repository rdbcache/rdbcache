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

    public AnyKey() {
    }

    public AnyKey(KeyInfo keyInfo) {
        add(keyInfo);
    }

    public AnyKey(List<KeyInfo> keyInfos) {
        for (KeyInfo keyInfo: keyInfos) {
            add(keyInfo);
        }
    }

    public KeyInfo getKey() {
        return getKey(0);
    }

    public KeyInfo getKey(int index) {
        if (index >= size()) {
            return null;
        }
        return get(index);
    }

    public void setKey(KeyInfo keyInfo) {
        set(0, keyInfo);
    }

    public void setKey(int index, KeyInfo keyInfo) {
        if (index >= size()) {
            throw new ServerErrorException("setKey index out of range");
        }
        set(index, keyInfo);
    }

    public KeyInfo getAny() {
        return getAny(0);
    }

    public KeyInfo getAny(int index) {
        if (size() == 0) {
            KeyInfo keyInfo = new KeyInfo();
            add(keyInfo);
            return keyInfo;
        }
        if (size() == 1) {
            return get(0);
        }
        if (index >= size()) {
            throw new ServerErrorException("getAny index out of range");
        }
        return get(index);
    }

    public AnyKey getAnyKey(int index) {
        return new AnyKey(getAny(index));
    }
}