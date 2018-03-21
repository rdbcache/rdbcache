package com.rdbcache.configs;

import com.rdbcache.exceptions.ServerErrorException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URL;

@ComponentScan(basePackages = { "com.rdbcache.*" })
@ConfigurationProperties(prefix = "spring.redis")
@PropertySources({
        @PropertySource("classpath:application.properties"),
        @PropertySource(value = "./application.properties", ignoreResourceNotFound = true)
})
public class RedisProperties {

    private String host;

    private Integer port;

    private String password;

    private Integer timeout;

    public static class Pool {

        private Integer maxActive;

        private Integer maxIdle;

        private Integer minIdle;

        private Integer maxWait;

        public Integer getMaxActive() {
            return maxActive;
        }

        public void setMaxActive(Integer maxActive) {
            this.maxActive = maxActive;
        }

        public Integer getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(Integer maxIdle) {
            this.maxIdle = maxIdle;
        }

        public Integer getMinIdle() {
            return minIdle;
        }

        public void setMinIdle(Integer minIdle) {
            this.minIdle = minIdle;
        }

        public Integer getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Integer maxWait) {
            this.maxWait = maxWait;
        }
    }

    private Pool pool;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        if (host == null || host.length() == 0) {
            return;
        }
        throw new ServerErrorException("spring.redis.host is not supported. use spring.redis.url instead");
    }

    public void setUrl(String url) {
        try {
            if (!url.substring(0, 5).equalsIgnoreCase("redis")) {
                throw new ServerErrorException("redis protocol is expected");
            }
            String httpUrl = "http"+url.substring(5);
            URL urlObj = new URL(httpUrl);
            host = urlObj.getHost();
            port = urlObj.getPort();
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServerErrorException(e.getCause().getMessage());
        }
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        if (port == null || port == 0) {
            return;
        }
        throw new ServerErrorException("spring.redis.port is not supported. use spring.redis.url instead");
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }

    public void configure(JedisConnectionFactory factory) {
        
        if (host != null) {
            factory.setHostName(host);
        } else {
            factory.setHostName("localhost");
        }
        if (port != null) {
            factory.setPort(port);
        } else {
            factory.setPort(6379);
        }
        if (password != null) {
            factory.setPassword(password);
        }
        if (timeout != null) {
            factory.setTimeout(timeout);
        }

        factory.setUseSsl(false);
        factory.setUsePool(true);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setBlockWhenExhausted(true);

        if (pool.maxActive != null) {
            int maxTotal = pool.maxActive;
            if (pool.minIdle != null) {
                maxTotal += pool.minIdle;
            } else {
                maxTotal += 2;
            }
            poolConfig.setMaxTotal(maxTotal);
        } else {
            poolConfig.setMaxTotal(18);
        }
        if (pool.maxIdle != null) {
            poolConfig.setMaxIdle(pool.maxIdle);
        } else {
            poolConfig.setMaxIdle(8);
        }
        if (pool.minIdle != null) {
            poolConfig.setMinIdle(pool.minIdle);
        } else {
            poolConfig.setMinIdle(2);
        }
        if (pool.maxWait != null) {
            poolConfig.setMaxWaitMillis(pool.maxWait);
        } else {
            poolConfig.setMaxWaitMillis(60000);
        }
        factory.setPoolConfig(poolConfig);
    }
}
