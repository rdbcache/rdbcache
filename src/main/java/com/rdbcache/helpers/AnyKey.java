package com.rdbcache.helpers;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.models.KeyInfo;

import java.util.ArrayList;
import java.util.List;

public class AnyKey {

    private List<KeyInfo> keyInfos;

    public AnyKey() {
        keyInfos = new ArrayList<>();;
    }

    public AnyKey(KeyInfo keyInfo) {
        keyInfos = new ArrayList<>();
        keyInfos.add(keyInfo);
    }

    public AnyKey(List<KeyInfo> keyInfos) {
        this.keyInfos = keyInfos;
    }

    public KeyInfo getKey() {
        if (keyInfos == null || keyInfos.size() == 0) {
            return null;
        }
        return keyInfos.get(0);
    }

    public KeyInfo getKey(int index) {
        if (keyInfos == null || index >= keyInfos.size()) {
            return null;
        }
        return keyInfos.get(index);
    }

    public void setKey(KeyInfo keyInfo) {
        if (keyInfos == null) {
            keyInfos = new ArrayList<>();
        }
        keyInfos.set(0, keyInfo);
    }

    public void setKey(int index, KeyInfo keyInfo) {
        if (keyInfos == null || index >= keyInfos.size()) {
            return;
        }
        keyInfos.set(index, keyInfo);
    }

    public List<KeyInfo> getKeys() {
        if (keyInfos == null) {
            keyInfos = new ArrayList<>();
        }
        return keyInfos;
    }

    public int size() {
        if (keyInfos == null) {
            return 0;
        }
        return keyInfos.size();
    }

    public KeyInfo getAny() {
        if (keyInfos == null) {
            keyInfos = new ArrayList<>();
        }
        if (keyInfos.size() == 0) {
            KeyInfo keyInfo = new KeyInfo();
            keyInfos.add(keyInfo);
            return keyInfo;
        }
        return keyInfos.get(0);
    }

    public KeyInfo getAny(int index) {
        if (keyInfos == null) {
            keyInfos = new ArrayList<>();
        }
        if (keyInfos.size() == 0) {
            KeyInfo keyInfo = new KeyInfo();
            keyInfos.add(keyInfo);
            return keyInfo;
        }
        if (keyInfos.size() == 1) {
            return keyInfos.get(0);
        }
        if (index >= keyInfos.size()) {
            throw new ServerErrorException("getAny index out of range");
        }
        return keyInfos.get(index);
    }

    public void add(KeyInfo keyInfo) {
        if (keyInfos == null) {
            keyInfos = new ArrayList<>();
        }
        keyInfos.add(keyInfo);
    }

    public boolean hasKey(int index) {
        if (keyInfos == null || index >= keyInfos.size()) {
            return false;
        }
        if (keyInfos.get(index) == null) {
            return false;
        }
        return true;
    }
}
