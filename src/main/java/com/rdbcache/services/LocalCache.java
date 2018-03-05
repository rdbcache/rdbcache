/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Cached;
import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LocalCache extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalCache.class);

    private final Map<String, Cached> cache = new ConcurrentHashMap<String, Cached>();

    public void put(String key, Cached cached) {
        cache.put(key, cached);
    }

    public void put(String key, Object object) {
        Cached cached = new Cached(object);
        cache.put(key, cached);
    }

    public void put(String key, Object object, Long timeToLive) {
        Cached cached = new Cached(object, timeToLive);
        cache.put(key, cached);
    }

    public Object put(String key, Long timeToLive, Callable<Object> refreshable) {
        if (refreshable == null) return null;
        Object object = null;
        try {
            object = refreshable.call();
        } catch (Exception e) {
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            throw new ServerErrorException(msg);
        }
        Cached cached = new Cached(object, timeToLive);
        cached.setRefreshable(refreshable);
        cache.put(key, cached);
        return object;
    }

    public Object get(String key) {
        Cached cached = cache.get(key);
        if (cached == null) {
            return null;
        }
        if (cached.isTimeout()) {
            cache.remove(key);
            return null;
        } else {
            return cached.getObject();
        }
    }

    public boolean containsKey(String key) {
        if (!cache.containsKey(key)) {
            return false;
        }
        Cached cached = cache.get(key);
        if (cached.isTimeout()) {
            cache.remove(key);
            return false;
        } else {
            return true;
        }
    }

    public void remove(String key) {
        cache.remove(key);
    }

    public void putKeyInfo(String key, KeyInfo keyInfo) {
        KeyInfo clone = null;
        if (keyInfo != null) {
            clone = keyInfo.clone();
            keyInfo.setIsNew(false);
        }

        Long ttl = Cfg.getKeyInfoCacheTTL();
        if (ttl > keyInfo.getTTL()) ttl = keyInfo.getTTL();

        Cached cached = new Cached(clone, ttl * 1000l);
        cache.put("keyinfo::" + key, cached);
    }

    public KeyInfo getKeyInfo(String key) {
        KeyInfo keyInfo = (KeyInfo) get("keyinfo::" + key);
        if (keyInfo == null) return null;
        KeyInfo clone = keyInfo.clone();
        clone.setIsNew(false);
        return clone;
    }

    public boolean getKeyInfos(List<String> keys, List<KeyInfo> keyInfos) {
        boolean findAll = true;
        for (String key: keys) {
            KeyInfo keyInfo = (KeyInfo) get("keyinfo::" + key);
            if (keyInfo == null) {
                findAll = false;
                keyInfos.add(null);
            } else {
                KeyInfo clone = keyInfo.clone();
                clone.setIsNew(false);
                keyInfos.add(clone);
            }
        }
        return findAll;
    }

    public void removeKeyInfo(String key) {
        cache.remove("keyinfo::" + key);
    }

    public void removeAllKeyInfo() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("keyinfo::")) {
                cache.remove(key);
            }
        }
    }

    public void putData(String key, Map map, KeyInfo keyInfo) {
        if (Cfg.getDataMaxCacheTLL() <= 0L) {
            return;
        }
        Long ttl = Cfg.getDataMaxCacheTLL();
        if (ttl > keyInfo.getTTL()) ttl = keyInfo.getTTL();
        Cached cached = new Cached(map, ttl * 1000l);
        cache.put("datamap::" + key, cached);
    }

    public void updateData(String key, Map<String, Object> update, KeyInfo keyInfo) {
        if (update == null || update.size() == 0 || Cfg.getDataMaxCacheTLL() <= 0L) {
            return;
        }
        Cached cached = cache.get("datamap::" + key);
        if (cached == null) {
            return;
        }
        if (cached.isTimeout()) {
            cache.remove(key);
            return;
        }
        Map<String, Object> map = (Map<String, Object>) cached.getObject();
        if (map == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : update.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    public Map<String, Object> getData(String key) {
        return (Map<String, Object>) get("datamap::" + key);
    }

    public void removeData(String key) {
        cache.remove("datamap::" + key);
    }

    public void removeAllData() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("datamap::")) {
                cache.remove(key);
            }
        }
    }

    public void removeAllTable() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("table_")) {
                cache.remove(key);
            }
        }
    }

    public void removeAll() {
        cache.clear();
    }

    @Override
    public void run() {

        LOGGER.info("LocalCache is running on thread " + getName());

        List<String> refreshKeys = new ArrayList<String>();
        List<String> timeoutKeys = new ArrayList<String>();

        SortedSet<Pair<String, Long>> lastAccessSortedKeys = new TreeSet<>(new Comparator<Pair<String, Long>>(){
            @Override
            public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
                return (int) (o1.getValue() - o2.getValue());
            }
        });

        while (true) {

            try {

                refreshKeys.clear();
                timeoutKeys.clear();
                lastAccessSortedKeys.clear();
                
                Thread.sleep(Cfg.getRecycleSchedule() * 1000L);

                for (Map.Entry<String, Cached> entry: cache.entrySet()) {
                    Cached cached = entry.getValue();
                    if (cached.isRefreshable() && cached.isAlmostTimeout()) {
                        refreshKeys.add(entry.getKey());
                    } else if (cached.isTimeout()) {
                        timeoutKeys.add(entry.getKey());
                    }
                }

                for (String key: timeoutKeys) {
                    cache.remove(key);
                }

                for (String key: refreshKeys) {

                    Cached cached = cache.get(key);
                    if (cached == null) {
                        continue;
                    }
                    Cached cachedClone = cached.clone();

                    Callable<Object> refreshable = cachedClone.getRefreshable();
                    if (refreshable == null) {
                        continue;
                    }
                    try {
                        Object object = refreshable.call();
                        if (object == null) {
                            continue;
                        }
                        cachedClone.setObject(object);
                        put(key, cachedClone);
                    } catch (Exception e) {
                        String msg = e.getCause().getMessage();
                        LOGGER.error(msg);
                        e.printStackTrace();
                    }
                }

                if (cache.size() < Cfg.getMaxCacheSize() * 3 / 4) {
                    continue;
                }

                for (Map.Entry<String, Cached> entry: cache.entrySet()) {
                    Cached object = entry.getValue();
                    Pair<String, Long> context = new Pair<>(entry.getKey(), object.getLastAccessAt());
                    lastAccessSortedKeys.add(context);
                }

                for (Pair<String, Long> context: lastAccessSortedKeys) {
                    cache.remove(context.getKey());
                    if (cache.size() < Cfg.getMaxCacheSize() * 3 / 4) {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }
        }

    }
}

