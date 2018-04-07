/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.commons.helpers.*;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class CacheOps extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheOps.class);

    private Long recycleSecs = PropCfg.getCacheRecycleSecs();

    private Long maxCacheSize = PropCfg.getMaxCacheSize();

    private Long keyMinCacheTTL = PropCfg.getKeyMinCacheTTL();

    private Long dataMaxCacheTLL = PropCfg.getDataMaxCacheTLL();

    private ConcurrentHashMap<String, Cached> cache = null;

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        recycleSecs = PropCfg.getCacheRecycleSecs();
        maxCacheSize = PropCfg.getMaxCacheSize();
        keyMinCacheTTL = PropCfg.getKeyMinCacheTTL();
        dataMaxCacheTLL = PropCfg.getDataMaxCacheTLL();

        if (cache == null) {
            initializeCache();
        }
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
        start();
    }

    @Override
    public synchronized void start() {
        if (cache == null) {
            initializeCache();
        }
        super.start();
    }

    private void initializeCache() {
        int initCapacity = maxCacheSize.intValue();
        int concurrentLevel = (initCapacity / 256 < 32 ? 32 : initCapacity / 256);
        cache = new ConcurrentHashMap<String, Cached>(
                initCapacity, 0.75f, concurrentLevel);
    }

    public Long getRecycleSecs() {
        return recycleSecs;
    }

    public void setRecycleSecs(Long recycleSecs) {
        this.recycleSecs = recycleSecs;
    }

    public Long getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMaxCacheSize(Long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public Long getKeyMinCacheTTL() {
        return keyMinCacheTTL;
    }

    public void setKeyMinCacheTTL(Long keyMinCacheTTL) {
        this.keyMinCacheTTL = keyMinCacheTTL;
    }

    public Long getDataMaxCacheTLL() {
        return dataMaxCacheTLL;
    }

    public void setDataMaxCacheTLL(Long dataMaxCacheTLL) {
        this.dataMaxCacheTLL = dataMaxCacheTLL;
    }

    public void put(String key, Map<String, Object> map) {
        cache.put(key, new Cached(map));
    }

    public void put(String key, Map<String, Object> map, long timeToLive) {
        cache.put(key, new Cached(map, timeToLive));
    }

    public Map<String, Object> put(String key, Long timeToLive, Callable<Map<String, Object>> refreshable) {
        try {
            Map<String, Object> map = refreshable.call();
            if (map == null) {
                return null;
            }
            Cached cached = new Cached(map, timeToLive);
            cached.refreshable = refreshable;
            cache.put(key, cached);
            return map;
        } catch (Exception e) {
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            throw new ServerErrorException(msg);
        }
    }

    public Map<String, Object> update(String key, Map<String, Object> update) {
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
                    Map<String, Object> map = cached.refreshable.call();
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
            if (map != null) {
                for (Map.Entry<String, Object> entry : update.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
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
                    Map<String, Object> map = cached.refreshable.call();
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

    public KeyInfo putKeyInfo(KvIdType idType, KeyInfo keyInfo) {
        if (keyMinCacheTTL <= 0l) {
            return null;
        }
        Long ttl = keyInfo.getExpireTTL();
        if (ttl < keyMinCacheTTL) ttl = keyMinCacheTTL;
        Map<String, Object> map = Utils.toMap(keyInfo);
        String hashKey = "keyInfo::"+ idType.getType() + ":" + idType.getId();
        put(hashKey, map, ttl * 1000);
        return keyInfo;
    }

    public KeyInfo getKeyInfo(KvIdType idType) {
        String hashKey = "keyInfo::"+ idType.getType() + ":" + idType.getId();
        Map<String, Object> map = get(hashKey);
        if (map == null) {
            return null;
        }
        KeyInfo keyInfo = Utils.toPojo(map, KeyInfo.class);
        return keyInfo;
    }

    public boolean containsKeyInfo(KvIdType idType) {
        String hashKey = "keyInfo::"+ idType.getType() + ":" + idType.getId();
        return containsKey(hashKey);
    }

    public void removeKeyInfo(KvIdType idType) {
        String hashKey = "keyInfo::"+ idType.getType() + ":" + idType.getId();
        cache.remove(hashKey);
    }

    public void removeKeyInfo(List<KvPair> pairs) {
        for (KvPair pair: pairs) {
            removeKeyInfo(pair.getIdType());
        }
    }

    public void putData(KvPair pair, KeyInfo keyInfo) {
        if (dataMaxCacheTLL <= 0L) {
            return;
        }
        Long ttl = keyInfo.getExpireTTL();
        if (ttl > dataMaxCacheTLL) ttl = dataMaxCacheTLL;
        String hashKey = pair.getType() + "::" + pair.getId();
        put(hashKey, pair.getDataClone(), ttl * 1000);
    }

    public void updateData(KvPair pair) {
        Map<String, Object> update = pair.getData();
        if (dataMaxCacheTLL <= 0L || update.size() == 0) {
            return ;
        }
        String type = pair.getType();
        String hashKey = pair.getType() + "::" + pair.getId();
        update(hashKey, update);
    }

    public Map<String, Object> getData(KvIdType idType) {
        String hashKey = idType.getType() + "::" + idType.getId();
        return (Map<String, Object>) get(hashKey);
    }

    public boolean containsData(KvIdType idType) {
        String hashKey = idType.getType() + "::" + idType.getId();
        return containsKey(hashKey);
    }

    public void removeData(KvIdType idType) {
        String hashKey = idType.getType() + "::" + idType.getId();
        cache.remove(hashKey);
    }

    public void removeKeyAndData(KvPair pair) {
        KvIdType idType = pair.getIdType();
        removeKeyInfo(idType);
        removeData(idType);
    }

    public void removeKeyAndData(List<KvPair> pairs) {
        for (KvPair pair: pairs) {
            removeKeyAndData(pair);
        }
    }

    public Map<String, Object> listAllKeyInfo(String typePrefix) {
        Map<String, Object> map = new LinkedHashMap<>();
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (typePrefix == null || typePrefix.length() == 0) {
                if (!key.startsWith("keyInfo::")) {
                    continue;
                }
            } else {
                if (!key.startsWith("keyInfo::"+typePrefix)) {
                    continue;
                }
            }
            Object object = get(key);
            if (object == null) continue;
            map.put(key, object);
        }
        return map;
    }

    public void removeAllKeyInfo(String typePrefix) {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("keyInfo::")) {
                if (typePrefix == null || typePrefix.length() == 0) {
                    if (!key.startsWith("keyInfo::")) {
                        continue;
                    }
                } else {
                    if (!key.startsWith("keyInfo::"+typePrefix)) {
                        continue;
                    }
                }
                cache.remove(key);
            }
        }
    }

    public Map<String, Object> listAllData(String type) {
        Map<String, Object> map = new LinkedHashMap<>();
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (type == null || type.length() == 0) {
                if (key.startsWith("keyInfo::") || key.startsWith("table_")) {
                    continue;
                }
            } else {
                if (!key.startsWith(type + "::")) {
                    continue;
                }
            }
            Object value = get(key);
            if (value == null) {
                continue;
            }
            map.put(key, value);
        }
        return map;
    }

    public void removeAllData(String type) {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (type == null || type.length() == 0) {
                if (key.startsWith("keyInfo::") || key.startsWith("table_")) {
                    continue;
                }
            } else {
                if (!key.startsWith(type + "::")) {
                    continue;
                }
            }
            cache.remove(key);
        }
    }

    public void removeAllKeyAndData() {
        Map<String, Object> map = new LinkedHashMap<>();
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (key.startsWith("table_")) {
                continue;
            }
            cache.remove(key);
        }
    }

    public Map<String, Object> listAllTables() {
        Map<String, Object> map = new LinkedHashMap<>();
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (!key.startsWith("table_")) {
                continue;
            }
            Object value = get(key);
            if (value == null) {
                continue;
            }
            map.put(key, value);
        }
        return map;
    }

    public void removeAllTables() {
        Set<String> keys = cache.keySet();
        for (String key: keys) {
            if (!key.startsWith("table_")) {
                continue;
            }
            cache.remove(key);
        }
    }

    public void removeAll() {
        cache.clear();
    }

    private List<String> refreshKeys = new ArrayList<String>();
    private List<String> timeoutKeys = new ArrayList<String>();

    private SortedSet<KeyLastAccess> lastAccessSortedKeys = new TreeSet<>(new Comparator<KeyLastAccess>(){
        @Override
        public int compare(KeyLastAccess o1, KeyLastAccess o2) {
            return (int) (o1.lastAccess - o2.lastAccess);
        }
    });

    private boolean isRunning = false;

    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void interrupt() {
        isRunning = false;
        super.interrupt();
    }

    @Override
    public void run() {

        isRunning = true;

        LOGGER.debug("CacheOps is running on thread " + getName());

        while (isRunning) {

            try {

                try {
                    Thread.sleep(recycleSecs * 1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!isRunning) break;

                timeoutKeys.clear();
                refreshKeys.clear();
                lastAccessSortedKeys.clear();

                final AtomicInteger atomicInteger = new AtomicInteger(0);
                cache.forEach(1, (key, cached) -> {
                    if (key == null || cached == null) return;
                    atomicInteger.incrementAndGet();
                    if (cached.isRefreshable()) {
                        if (cached.isAlmostTimeout()) refreshKeys.add(key);
                    } else if (cached.isTimeout()) {
                        timeoutKeys.add(key);
                    } else {
                        lastAccessSortedKeys.add(new KeyLastAccess(key, cached.lastAccessAt));
                    }
                });

                int size = atomicInteger.intValue();
                long almostMax = 3 * maxCacheSize / 4;

                LOGGER.trace("recycle start -> cache size: " + size + ", max: " + maxCacheSize  + ", almostMax: " + almostMax);

                LOGGER.trace("timeoutKeys size: " + timeoutKeys.size());

                for (String key: timeoutKeys) {
                    size--;
                    if (key == null) continue;
                    cache.remove(key);
                    LOGGER.trace("timeout key: " + key);
                }
                timeoutKeys.clear();

                LOGGER.trace("refreshKeys size: " + refreshKeys.size());

                for (String key: refreshKeys) {
                    if (key == null) continue;
                    Cached cached = cache.get(key);
                    if (cached == null) continue;
                    Cached clone = cached.clone();
                    Utils.getExcutorService().submit(() -> {
                        try {
                            Map<String, Object> map = clone.refreshable.call();
                            clone.setMap(map);
                        } catch (Exception e) {
                            String msg = e.getCause().getMessage();
                            LOGGER.error(msg);
                            e.printStackTrace();
                        }
                        cache.put(key, clone);
                        LOGGER.trace("refresh key: " + key);
                    });
                }
                refreshKeys.clear();

                if (size <= almostMax) {
                    continue;
                }

                LOGGER.trace("reduce cache size");

                LOGGER.trace("lastAccessSortedKeys size: " + lastAccessSortedKeys.size());

                for (KeyLastAccess keyLastAccess: lastAccessSortedKeys) {
                    size--;
                    if (keyLastAccess == null || keyLastAccess.key == null) continue;
                    cache.remove(keyLastAccess.key);
                    LOGGER.trace("remove key (" + keyLastAccess.lastAccess + "): " + keyLastAccess.key);
                    if (size <= almostMax) {
                        break;
                    }
                }
                lastAccessSortedKeys.clear();

            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }
        }

        isRunning = false;
    }

    class KeyLastAccess {

        String key;
        long lastAccess;

        KeyLastAccess(String key, long lastAccess) {
            this.key = key;
            this.lastAccess = lastAccess;
        }
    }

    class Cached implements Cloneable {

        private Map<String, Object> map;

        long createdAt;

        long timeToLive = 900000L;  // default 15 minutes

        long lastAccessAt;

        Callable<Map<String, Object>> refreshable;

        Cached() {
            createdAt = System.currentTimeMillis();
            lastAccessAt = System.nanoTime();
        }

        Cached(Map<String, Object> map) {
            createdAt = System.currentTimeMillis();
            lastAccessAt = System.nanoTime();
            this.map = map;
        }

        Cached(Map<String, Object> map, long timeToLive) {
            createdAt = System.currentTimeMillis();
            lastAccessAt = System.nanoTime();
            this.map = map;
            this.timeToLive = timeToLive;
        }

        synchronized Map<String, Object> getMap() {
            lastAccessAt = System.nanoTime();
            return map;
        }

        synchronized void setMap(Map<String, Object> map) {
            lastAccessAt = System.nanoTime();
            this.map = map;
        }

        boolean isRefreshable() {
            return (refreshable != null);
        }

        boolean isTimeout() {
            long now = System.currentTimeMillis();
            if (now > createdAt + timeToLive) {
                return true;
            } else {
                return false;
            }
        }

        boolean isAlmostTimeout() {
            long now = System.currentTimeMillis();
            if (now > createdAt + timeToLive * 3 / 4) {
                return true;
            } else {
                return false;
            }
        }

        void renew() {
            lastAccessAt = System.nanoTime();
            createdAt = System.currentTimeMillis();
        }

        protected synchronized Cached clone() {
            Cached clone = new Cached();
            if (map != null) {
                clone.map = new LinkedHashMap<>(map);
            }
            clone.createdAt = createdAt;
            clone.timeToLive = timeToLive;
            clone.refreshable = refreshable;
            return clone;
        }
    }
}

