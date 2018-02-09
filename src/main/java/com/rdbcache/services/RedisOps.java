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

import com.rdbcache.helpers.AppCtx;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Service
public class RedisOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOps.class);

    // make sure config set notify-keyspace-events Ex
    //
    public void ensureNotifyKeySpaceEventsEx() {

        RedisConnection connection = AppCtx.getRedisTemplate().getConnectionFactory().getConnection();

        List<String> items = connection.getConfig("notify-keyspace-events");

        String config = "";

        Iterator<String> iterator = items.iterator();
        while (iterator.hasNext()) {
            String item = iterator.next();
            if (item.equals("notify-keyspace-events")) {
                config = iterator.next();
                break;
            }
        }

        if (config.contains("E") && (config.contains("A") || config.contains("x"))) {
            return;
        }

        if (!config.contains("E")) {
            config += "E";
        }
        if (!config.contains("A") && !config.contains("x")) {
            config += "x";
        }
        connection.setConfig("notify-keyspace-events", config);
    }

}
