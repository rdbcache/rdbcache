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

package com.rdbcache.services;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class ExpireOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpireOps.class);

    @Autowired
    private StringRedisTemplate redisTemplate;

    private ValueOperations valueOps;

    @PostConstruct
    public void init() {
        valueOps = redisTemplate.opsForValue();
    }

    // set up expire key
    //
    // expire = X,  it schedules an event in X seconds, only if not such event exists.
    //              The expiration event will happen at X seconds.
    //
    // expire = +X, it schedules an event in X seconds always, even if such event exists.
    //              It may use to delay the event, if next call happens before X seconds.
    //
    // expire = -X, it schedules a repeat event to occur every X seconds.
    //
    // expire = 0,  it removes existing event and not to set any event
    //
    public void setExpireKey(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        String table = keyInfo.getTable();

        LOGGER.trace("setExpireKey: " + key + " expire: " +keyInfo.getExpire());

        // get existing expire key
        String expKey = Cfg.getEventPrefix() + "::" + key;

        StopWatch stopWatch = context.startStopWatch("redis", "redisTemplate.hasKey");
        Set<String> expKeys = AppCtx.getRedisTemplate().keys(expKey + "::*");
        if (stopWatch != null) stopWatch.stopNow();

        String expire = keyInfo.getExpire();
        Long expValue = Long.valueOf(expire);

        // remove existing expire key
        if (expKeys.size() > 0) {
            if (expValue <= 0L || expire.startsWith("+")) {

                stopWatch = context.startStopWatch("redis", "redisTemplate.delete");
                AppCtx.getRedisTemplate().delete(expKeys);
                if (stopWatch != null) stopWatch.stopNow();

            } else {
                // for unsigned expire, event existed, no update
                return;
            }
        }

        // zero means no expiration
        if (expValue == 0L) {
            return;
        }

        if (keyInfo.getIsNew()) {
            AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);
        }

        if (expValue < 0) {
            expValue = -expValue;
        }

        expKey += "::" + context.getTraceId();

        LOGGER.trace("set event: " + expKey + " expire: " + expValue);

        stopWatch = context.startStopWatch("redis", "valueOps.set");
        valueOps.set(expKey, expire, expValue, TimeUnit.SECONDS);
        if (stopWatch != null) stopWatch.stopNow();
    }

    /**
     * To process key expired message
     *
     * @param message key expired message
     */
    public void onMessage(String message) {

        LOGGER.trace("Received message: " + message);

        if (!message.startsWith(Cfg.getEventPrefix())) {
            return;
        }

        String[] parts = message.split("::");

        if (parts.length < 3) {
            LOGGER.error("invalid message format");
            return;
        }

        String key = parts[1];
        String traceId = parts[2];

        Context context = new Context(key, traceId);

        if (Cfg.getEnableMonitor()) context.enableMonitor(message, "event", key);

        KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
        if (keyInfo == null) {
            String msg = "keyInfo not found";
            LOGGER.error(msg);
            context.logTraceMessage(msg);
            context.stopFirstStopWatch();
            return;
        }

        LOGGER.trace(keyInfo.toString());

        Long expire = Long.valueOf(keyInfo.getExpire());

        if (expire > 0) {
            if (AppCtx.getRedisRepo().findOne(context, keyInfo)) {

                final KeyInfo finalKeyInfo = keyInfo;
                AsyncOps.getExecutor().submit(() -> {
                    AppCtx.getDbaseRepo().saveOne(context, finalKeyInfo);
                    AppCtx.getRedisRepo().delete(context, finalKeyInfo);
                    AppCtx.getKeyInfoRepo().deleteOne(context);
                });

            } else {
                String msg = "failed to find key from redis for " + key;
                LOGGER.error(msg);
                context.logTraceMessage(msg);
            }
            context.stopFirstStopWatch();
            return;
        }

        if (expire < 0) {
            if (AppCtx.getDbaseRepo().findOne(context, keyInfo)) {

                final KeyInfo finalKeyInfo = keyInfo;
                AsyncOps.getExecutor().submit(() -> {
                    AppCtx.getRedisRepo().saveOne(context, finalKeyInfo);
                    setExpireKey(context, finalKeyInfo);
                });

            } else {
                String msg = "failed to find key from database for " + key;
                LOGGER.error(msg);
                context.logTraceMessage(msg);
            }
            context.stopFirstStopWatch();
            return;
        }

        context.stopFirstStopWatch();
    }
}
