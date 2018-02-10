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

import com.rdbcache.helpers.Config;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.KeyInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Service
public class TaskQueue extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueue.class);

    private ListOperations listOps;

    @Autowired
    StringRedisTemplate redisTemplate;

    private String taskQueue;

    @PostConstruct
    public void init() {
        taskQueue = Config.getHkeyPrefix() + ":queue";
        listOps = redisTemplate.opsForList();
    }

    @Override
    public void run() {

        LOGGER.info("Event Task Queue is running on thread " + getName());

        Boolean freshConnection = true;

        while (true) {

            try {

                if (freshConnection) {
                    AppCtx.getRedisOps().ensureNotifyKeySpaceEventsEx();
                    freshConnection = false;
                }

                String task = (String) listOps.leftPop(taskQueue, 0, TimeUnit.SECONDS);

                LOGGER.info("Received Task: " + task);

                String[] parts = task.split("::");

                if (parts.length < 3) {
                    LOGGER.error("invalid task format");
                    return;
                }

                String action = parts[0];
                String key = parts[1];
                String traceId = parts[2];

                Context context = new Context(key, traceId);

                if (Config.getEnableMonitor()) context.enableMonitor(task, "queue", action);

                KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
                if (keyInfo == null) {
                    keyInfo = new KeyInfo(Config.getDefaultExpire(), "value");
                }

                //...

                String msg = "unknown task action:" + action;
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                context.stopFirstStopWatch();

            } catch (RedisConnectionFailureException e) {

                LOGGER.warn("Connection failure occurred. Restarting task queue after 5000 ms");

                e.printStackTrace();

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                freshConnection = true;

            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }
        }
    }
}
