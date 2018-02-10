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

package com.rdbcache.configs;

import com.rdbcache.helpers.Config;
import com.rdbcache.helpers.NullableSerializer;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.services.ExpireOps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {

    @Bean
    JedisConnectionFactory redisConnectionFactory() {
        JedisConnectionFactory factory = new JedisConnectionFactory();

        if (Config.getRedisServer() != null) {
            factory.setHostName(Config.getRedisServer());
        } else {
            factory.setHostName("localhost");
        }
        if (Config.getRedisPort() != null) {
            factory.setPort(Config.getRedisPort());
        } else {
            factory.setPort(6379);
        }
        if (Config.getRedisPassword() != null) {
            factory.setPassword(Config.getRedisPassword());
        }
        if (Config.getRedisTimeout() != null) {
            factory.setTimeout(Config.getRedisTimeout());
        }
        factory.setUseSsl(false);

        factory.setUsePool(true);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        if (Config.getRedisPoolMaxActive() != null) {
            int maxTotal = Config.getRedisPoolMaxActive();
            if (Config.getRedisPoolMinIdle() != null) {
                maxTotal += Config.getRedisPoolMinIdle();
            } else {
                maxTotal += 2;
            }
            poolConfig.setMaxTotal(maxTotal);
        } else {
            poolConfig.setMaxTotal(18);
        }
        if (Config.getRedisPoolMaxIdle() != null) {
            poolConfig.setMaxIdle(Config.getRedisPoolMaxIdle());
        } else {
            poolConfig.setMaxIdle(8);
        }
        if (Config.getRedisPoolMinIdle() != null) {
            poolConfig.setMinIdle(Config.getRedisPoolMinIdle());
        } else {
            poolConfig.setMinIdle(2);
        }
        if (Config.getRedisPoolMaxWait() != null) {
            poolConfig.setMaxWaitMillis(Config.getRedisPoolMaxWait());
        } else {
            poolConfig.setMaxWaitMillis(60000);
        }
        factory.setPoolConfig(poolConfig);

        factory.afterPropertiesSet();
        return factory;
    }

    @Bean
    ExpireOps expireOps() {
        return new ExpireOps();
    }

    @Bean
    RedisTemplate<String, KeyInfo> keyInfoTemplate() {

        RedisTemplate<String, KeyInfo> template =  new RedisTemplate<String, KeyInfo>();

        template.setConnectionFactory(redisConnectionFactory());

        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new Jackson2JsonRedisSerializer<KeyInfo>(KeyInfo.class));

        return template;
    }

    @Bean
    public StringRedisTemplate redisTemplate() {
        StringRedisTemplate template = new StringRedisTemplate();

        template.setConnectionFactory(redisConnectionFactory());
        template.setHashValueSerializer(new NullableSerializer());

        return template;
    }

    @Bean
    MessageListenerAdapter listenerAdapter() {
        return new MessageListenerAdapter(expireOps(), "onMessage");
    }

    @Bean
    RedisMessageListenerContainer container() {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory());
        container.addMessageListener(listenerAdapter(), new PatternTopic("__key*__:expired"));
        return container;
    }

}
