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

import com.rdbcache.helpers.NullableSerializer;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.services.ExpireOps;
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
    RedisProperties redisProperties() {
        return new RedisProperties();
    }

    @Bean
    JedisConnectionFactory redisConnectionFactory() {
        JedisConnectionFactory factory = new JedisConnectionFactory();

        RedisProperties properties = redisProperties();

        if (properties.getHost() != null) {
            System.out.println("properties.getHost() = "+properties.getHost());
            factory.setHostName(properties.getHost());
        } else {
            factory.setHostName("localhost");
        }
        if (properties.getPort() != null) {
            System.out.println("properties.getPort() = "+properties.getPort());
            factory.setPort(properties.getPort());
        } else {
            factory.setPort(6379);
        }
        if (properties.getPassword() != null) {
            System.out.println("properties.getPassword() = "+properties.getPassword());
            factory.setPassword(properties.getPassword());
        }
        if (properties.getTimeout() != null) {
            System.out.println("properties.getTimeout() = "+properties.getTimeout());
            factory.setTimeout(properties.getTimeout());
        }

        factory.setUseSsl(false);
        factory.setUsePool(true);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);

        if (properties.getPool().getMaxActive() != null) {
            System.out.println("properties.getPool().getMaxActive() = "+properties.getPool().getMaxActive());
            int maxTotal = properties.getPool().getMaxActive();
            if (properties.getPool().getMinIdle() != null) {
                maxTotal += properties.getPool().getMinIdle();
            } else {
                maxTotal += 2;
            }
            poolConfig.setMaxTotal(maxTotal);
        } else {
            poolConfig.setMaxTotal(18);
        }
        if (properties.getPool().getMaxIdle() != null) {
            System.out.println("properties.getPool().getMaxIdle() = "+properties.getPool().getMaxIdle());
            poolConfig.setMaxIdle(properties.getPool().getMaxIdle());
        } else {
            poolConfig.setMaxIdle(8);
        }
        if (properties.getPool().getMinIdle() != null) {
            System.out.println("properties.getPool().getMinIdle() = "+properties.getPool().getMinIdle());
            poolConfig.setMinIdle(properties.getPool().getMinIdle());
        } else {
            poolConfig.setMinIdle(2);
        }
        if (properties.getPool().getMaxWait() != null) {
            System.out.println("properties.getPool().getMaxWait() = "+properties.getPool().getMaxWait());
            poolConfig.setMaxWaitMillis(properties.getPool().getMaxWait());
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
