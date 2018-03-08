/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javafx.util.Pair;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LocalCache extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalCache.class);

    private Long recycleMills = Cfg.getRecycleSchedule() * 1000L;

    private Long maxCacheSize = Cfg.getMaxCacheSize();

    private Long keyInfoCacheTTL = Cfg.getKeyInfoCacheTTL();

    private Long dataMaxCacheTLL = Cfg.getDataMaxCacheTLL();

    private final Map<String, Cached> cache = new ConcurrentHashMap<String, Cached>();

    public Long getRecycleMills() {
        return recycleMills;
    }

    public void setRecycleMills(Long recycleMills) {
        this.recycleMills = recycleMills;
    }

    public Long getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMaxCacheSize(Long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public Long getKeyInfoCacheTTL() {
        return keyInfoCacheTTL;
    }

    public void setKeyInfoCacheTTL(Long keyInfoCacheTTL) {
        this.keyInfoCacheTTL = keyInfoCacheTTL;
    }

    public Long getDataMaxCacheTLL() {
        return dataMaxCacheTLL;
    }

    public void setDataMaxCacheTLL(Long dataMaxCacheTLL) {
        this.dataMaxCacheTLL = dataMaxCacheTLL;
    }

    public void put(String key, @NotNull Map<String, Object> map) {
        cache.put(key, new Cached(map));
    }

    public void put(String key, @NotNull Map<String, Object> map, long timeToLive) {
        cache.put(key, new Cached(map, timeToLive));
    }

    public Map<String, Object> put(String key, Long timeToLive, @NotNull Callable<Map<String, Object>> refreshable) {
        try {
            Map<String, Object> map = refreshable.call();
            if (map == null) {
                return null;
            }
            Cached cached = new Cached(map, timeToLive);
            cached.setRefreshable(refreshable);
            cache.put(key, cached);
            return map;
        } catch (Exception e) {
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            throw new ServerErrorException(msg);
        }
    }

    public Map<String, Object> update(String key, @NotNull Map<String, Object> update) {
        Cached cached = cache.get(key);
        if (cached == null) {
            return null;
        }
        if (cached.isTimeout()) {
            if (!cached.isRefreshable()) {
                cache.remove(key);
                return null;
            } else {
                try {
                    Map<String, Object> map = cached.getRefreshable().call();
                    if (map == null) {
                        cache.remove(key);
                        return null;
                    }
                    for (Map.Entry<String, Object> entry: update.entrySet()) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                    cached.setMap(map);
                    cached.renew();
                    return map;
                } catch (Exception e) {
                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    throw new ServerErrorException(msg);
                }
            }
        } else {
            Map<String, Object> map = cached.getMap();
            for (Map.Entry<String, Object> entry: update.entrySet()) {
                map.put(entry.getKey(), entry.getValue());
            }
            return map;
        }
    }

    public Map<String, Object> get(String key) {
        Cached cached = cache.get(key);
        if (cached == null) {
            return null;
        }
        if (cached.isTimeout()) {
            if (!cached.isRefreshable()) {
                cache.remove(key);
                return null;
            } else {
                try {
                    Map<String, Object> map = cached.getRefreshable().call();
                    cached.setMap(map);
                    cached.renew();
                    return map;
                } catch (Exception e) {
                    String msg = e.getCause().getMessage();
                    LOGGER.error(msg);
                    throw new ServerErrorException(msg);
                }
            }
        } else {
            return cached.getMap();
        }
    }

    public boolean containsKey(String key) {
        if (!cache.containsKey(key)) {
            return false;
        }
        Cached cached = cache.get(key);
        if (cached.isTimeout() && !cached.isRefreshable()) {
            cache.remove(key);
            return false;
        } else {
            return true;
        }
    }

    public void remove(String key) {
        cache.remove(key);
    }

    public void putKeyInfo(String key, @NotNull KeyInfo keyInfo) {
        if (keyInfoCacheTTL <= 0l) {
            return;
        }
        Long ttl = keyInfo.getTTL();
        if (ttl < keyInfoCacheTTL) ttl = keyInfoCacheTTL;
        put("key::" + key, keyInfo.toMap(), ttl * 1000);
    }

    public KeyInfo getKeyInfo(String key) {
        Map<String, Object> map = get("key::" + key);
        if (map == null) return null;
        KeyInfo keyInfo = new KeyInfo(map);
        return keyInfo;
    }

    public boolean getKeyInfos(List<String> keys, List<KeyInfo> keyInfos) {
        boolean findAll = true;
        for (String key: keys) {
            KeyInfo keyInfo = getKeyInfo(key);
            if (keyInfo == null) {
                findAll = false;
                keyInfos.add(null);
            } else {
                keyInfos.add(keyInfo);
            }
        }
        return findAll;
    }

    public void removeKeyInfo(String key) {
        cache.remove("key::" + key);
    }

    public void removeAllKeyInfo() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("key::")) {
                cache.remove(key);
            }
        }
    }

    public void putData(String key, Map<String, Object> map, KeyInfo keyInfo) {
        if (dataMaxCacheTLL <= 0L) {
            return;
        }
        Long ttl = keyInfo.getTTL();
        if (ttl > dataMaxCacheTLL) ttl = dataMaxCacheTLL;
        put("data::" + key, map, ttl * 1000);
    }

    public void updateData(String key, @NotNull Map<String, Object> update, KeyInfo keyInfo) {
        if (dataMaxCacheTLL <= 0L || update.size() == 0) {
            return ;
        }
        update("data::" + key, update);
    }

    public Map<String, Object> getData(String key) {
        return get("data::" + key);
    }

    public void removeData(String key) {
        cache.remove("data::" + key);
    }

    public void removeAllData() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("data::")) {
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
                
                Thread.sleep(recycleMills);

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
                    Cached clone = cached.clone();
                    Callable<Map<String, Object>> refreshable = clone.getRefreshable();
                    try {
                        Map<String, Object> map = refreshable.call();
                        clone.setMap(map);
                    } catch (Exception e) {
                        String msg = e.getCause().getMessage();
                        LOGGER.error(msg);
                        e.printStackTrace();
                    }
                    cache.put(key, clone);
                }

                if (cache.size() < maxCacheSize * 3 / 4) {
                    continue;
                }

                for (Map.Entry<String, Cached> entry: cache.entrySet()) {
                    Cached cached = entry.getValue();
                    Pair<String, Long> context = new Pair<>(entry.getKey(), cached.getLastAccessAt());
                    lastAccessSortedKeys.add(context);
                }

                for (Pair<String, Long> context: lastAccessSortedKeys) {
                    cache.remove(context.getKey());
                    if (cache.size() < maxCacheSize * 3 / 4) {
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

    class Cached implements Cloneable {

        private Map<String, Object> map;

        private Long createdAt;

        private Long timeToLive = 900000L;  // default 15 minutes

        private Long lastAccessAt;

        private Callable<Map<String, Object>> refreshable;

        public Cached(@NotNull Map<String, Object> map) {
            lastAccessAt = createdAt = System.currentTimeMillis();
            this.map = map;
        }

        public Cached(@NotNull Map<String, Object> map, Long timeToLive) {
            lastAccessAt = createdAt = System.currentTimeMillis();
            this.map = map;
            this.timeToLive = timeToLive;
        }

        public synchronized Map<String, Object> getMap() {
            lastAccessAt = System.currentTimeMillis();
            return map;
        }

        public synchronized void setMap(@NotNull Map<String, Object> map) {
            lastAccessAt = System.currentTimeMillis();
            this.map = map;
        }

        public boolean isRefreshable() {
            return (refreshable != null);
        }

        public Callable<Map<String, Object>> getRefreshable() {
            return refreshable;
        }

        public void setRefreshable(Callable<Map<String, Object>> refreshable) {
            this.refreshable = refreshable;
        }

        public Boolean isTimeout() {
            long now = System.currentTimeMillis();
            if (now > createdAt + timeToLive) {
                return true;
            } else {
                return false;
            }
        }

        public Boolean isAlmostTimeout() {
            long now = System.currentTimeMillis();
            if (now > createdAt + timeToLive * 3 / 4) {
                return true;
            } else {
                return false;
            }
        }

        public Cached clone() {
            try {
                Cached clone = (Cached) super.clone();
                clone.setMap(new LinkedHashMap<>(map));
                return clone;
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
                throw new ServerErrorException(e.getCause().getMessage());
            }
        }

        public Long getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
        }

        public Long getTimeToLive() {
            return timeToLive;
        }

        public void setTimeToLive(Long timeToLive) {
            this.timeToLive = timeToLive;
        }

        public Long getLastAccessAt() {
            return lastAccessAt;
        }

        public void setLastAccessAt(Long lastAccessAt) {
            this.lastAccessAt = lastAccessAt;
        }

        public void updateLastAccessAt() {
            this.lastAccessAt = System.currentTimeMillis();
        }

        public void renew() {
            lastAccessAt = createdAt = System.currentTimeMillis();
        }
    }
}

