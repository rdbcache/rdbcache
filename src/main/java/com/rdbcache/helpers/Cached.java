/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
