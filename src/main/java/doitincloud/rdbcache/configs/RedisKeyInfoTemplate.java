package doitincloud.rdbcache.configs;

import doitincloud.rdbcache.models.KeyInfo;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisKeyInfoTemplate extends RedisTemplate<String, KeyInfo> {
}
