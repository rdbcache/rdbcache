/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.helpers.AppCtx;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.ScriptExecutor;
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
