/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import java.util.concurrent.Callable;

public class Cached {

    private Object object;

    private Long createdAt;

    private Long timeToLive = 3600000L;  // default 1 hour

    private Long lastAccessAt;

    private Callable<Object> refreshable;

    public Cached(Object object) {
        lastAccessAt = createdAt = System.currentTimeMillis();
        this.object = object;
    }

    public Cached(Object object, Long timeToLive) {
        lastAccessAt = createdAt = System.currentTimeMillis();
        this.object = object;
        this.timeToLive = timeToLive;
    }

    public Object getObject() {
        lastAccessAt = System.currentTimeMillis();
        return object;
    }

    public void setObject(Object object) {
        lastAccessAt = System.currentTimeMillis();
        this.object = object;
    }

    public boolean isRefreshable() {
        return (refreshable != null);
    }

    public Callable<Object> getRefreshable() {
        return refreshable;
    }

    public void setRefreshable(Callable<Object> refreshable) {
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
        Cached newCached = new Cached(object);
        newCached.createdAt = createdAt;
        newCached.timeToLive = timeToLive;
        newCached.lastAccessAt = lastAccessAt;
        newCached.refreshable = refreshable;
        return newCached;
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
