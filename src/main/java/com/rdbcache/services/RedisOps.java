/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.configs.AppCtx;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
public class RedisOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOps.class);

    @PostConstruct
    public void init() {
    }

    @org.springframework.context.event.EventListener
    public void handleEvent(ContextRefreshedEvent event) {
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
    }

    // make sure config set notify-keyspace-events Ex
    //
    public void ensureNotifyKeySpaceEventsEx() {

        RedisConnection connection = AppCtx.getStringRedisTemplate().getConnectionFactory().getConnection();

        Properties properties = connection.getConfig("notify-keyspace-events");

        String config = "";

        for (String key: properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            if (value.equals("notify-keyspace-events")) {
                config = value;
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

        LOGGER.trace("setConfig notify-keyspace-events " + config);
    }
}
