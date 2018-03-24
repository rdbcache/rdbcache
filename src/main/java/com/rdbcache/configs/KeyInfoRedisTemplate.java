package com.rdbcache.configs;

import com.rdbcache.models.KeyInfo;
import org.springframework.data.redis.core.RedisTemplate;

public class KeyInfoRedisTemplate extends RedisTemplate<String, KeyInfo> {
}
