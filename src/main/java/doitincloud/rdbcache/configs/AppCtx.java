/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.configs;

import doitincloud.rdbcache.controllers.RdbcacheApis;
import doitincloud.commons.helpers.VersionInfo;
import doitincloud.rdbcache.repositories.*;
import doitincloud.rdbcache.services.*;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class AppCtx {

    private static ApplicationContext ctx;

    private static VersionInfo versionInfo;

    private static RdbcacheApis rdbcacheApis;

    private static AsyncOps asyncOps;

    private static DbaseOps dbaseOps;

    private static ExpireOps expireOps;

    private static CacheOps cacheOps;

    private static RedisOps redisOps;

    private static QueueOps queueOps;

    private static DbaseRepo dbaseRepo;

    private static KeyInfoRepo keyInfoRepo;

    private static RedisKeyInfoTemplate redisKeyInfoTemplate;

    private static KvPairRepo kvPairRepo;

    private static MonitorRepo monitorRepo;

    private static RedisRepo redisRepo;

    private static StopWatchRepo stopWatchRepo;

    private static JdbcTemplate jdbcTemplate;

    private static StringRedisTemplate stringRedisTemplate;

    public static ApplicationContext getApplicationContext() {
        return ctx;
    }

    public static void setApplicationContext(ApplicationContext ctx) {
        AppCtx.ctx = ctx;
    }

    public static VersionInfo getVersionInfo() {
        if (versionInfo == null) {
            versionInfo = new VersionInfo();
        }
        return versionInfo;
    }

    public static void setVersionInfo(VersionInfo versionInfo) {
        AppCtx.versionInfo = versionInfo;
    }

    public static RdbcacheApis getRdbcacheApis() {
        if (ctx != null && rdbcacheApis == null) {
            try {
                rdbcacheApis = ctx.getBean(RdbcacheApis.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rdbcacheApis;
    }

    public static void setRdbcacheApis(RdbcacheApis rdbcacheApis) {
        AppCtx.rdbcacheApis = rdbcacheApis;
    }

    public static AsyncOps getAsyncOps() {
        if (ctx != null && asyncOps == null) {
            try {
                asyncOps = ctx.getBean(AsyncOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return asyncOps;
    }

    public static void setAsyncOps(AsyncOps asyncOps) {
        AppCtx.asyncOps = asyncOps;
    }

    public static DbaseOps getDbaseOps() {
        if (ctx != null && dbaseOps == null) {
            try {
                dbaseOps = ctx.getBean(DbaseOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dbaseOps;
    }

    public static void setDbaseOps(DbaseOps dbaseOps) {
        AppCtx.dbaseOps = dbaseOps;
    }

    public static ExpireOps getExpireOps() {
        if (ctx != null && expireOps == null) {
            try {
                expireOps = ctx.getBean(ExpireOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return expireOps;
    }

    public static void setExpireOps(ExpireOps expireOps) {
        AppCtx.expireOps = expireOps;
    }

    public static RedisOps getRedisOps() {
        if (ctx != null && redisOps == null) {
            try {
                redisOps = ctx.getBean(RedisOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisOps;
    }

    public static void setRedisOps(RedisOps redisOps) {
        AppCtx.redisOps = redisOps;
    }

    public static CacheOps getCacheOps() {
        if (ctx != null && cacheOps == null) {
            try {
                cacheOps = ctx.getBean(CacheOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cacheOps;
    }

    public static void setCacheOps(CacheOps cache) {
        cacheOps = cache;
    }

    public static QueueOps getQueueOps() {
        if (ctx != null && queueOps == null) {
            try {
                queueOps = ctx.getBean(QueueOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queueOps;
    }

    public static void setQueueOps(QueueOps queueOps) {
        AppCtx.queueOps = queueOps;
    }

    public static DbaseRepo getDbaseRepo() {
        if (ctx != null && dbaseRepo == null) {
            try {
                dbaseRepo = ctx.getBean(DbaseRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dbaseRepo;
    }

    public static void setDbaseRepo(DbaseRepo dbaseRepo) {
        AppCtx.dbaseRepo = dbaseRepo;
    }

    public static KeyInfoRepo getKeyInfoRepo() {
        if (ctx != null && keyInfoRepo == null) {
            try {
                keyInfoRepo = ctx.getBean(KeyInfoRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return keyInfoRepo;
    }

    public static void setKeyInfoRepo(KeyInfoRepo keyInfoRepo) {
        AppCtx.keyInfoRepo = keyInfoRepo;
    }

    public static KvPairRepo getKvPairRepo() {
        if (ctx != null && kvPairRepo == null) {
            try {
                kvPairRepo = ctx.getBean(KvPairRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return kvPairRepo;
    }

    public static void setKvPairRepo(KvPairRepo kvPairRepo) {
        AppCtx.kvPairRepo = kvPairRepo;
    }

    public static RedisKeyInfoTemplate getRedisKeyInfoTemplate() {
        if (ctx != null && redisKeyInfoTemplate == null) {
            try {
                redisKeyInfoTemplate = ctx.getBean(RedisKeyInfoTemplate.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisKeyInfoTemplate;
    }

    public static void setRedisKeyInfoTemplate(RedisKeyInfoTemplate template) {
        AppCtx.redisKeyInfoTemplate = template;
    }

    public static MonitorRepo getMonitorRepo() {
        if (ctx != null && monitorRepo == null) {
            try {
                monitorRepo = ctx.getBean(MonitorRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return monitorRepo;
    }

    public static void setMonitorRepo(MonitorRepo monitorRepo) {
        AppCtx.monitorRepo = monitorRepo;
    }

    public static RedisRepo getRedisRepo() {
        if (ctx != null && redisRepo == null) {
            try {
                redisRepo = ctx.getBean(RedisRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisRepo;
    }

    public static void setRedisRepo(RedisRepo redisRepo) {
        AppCtx.redisRepo = redisRepo;
    }

    public static StopWatchRepo getStopWatchRepo() {
        if (ctx != null && stopWatchRepo == null) {
            try {
                stopWatchRepo = ctx.getBean(StopWatchRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return stopWatchRepo;
    }

    public static void setStopWatchRepo(StopWatchRepo stopWatchRepo) {
        AppCtx.stopWatchRepo = stopWatchRepo;
    }

    public static JdbcTemplate getJdbcTemplate() {
        if (ctx != null && jdbcTemplate == null) {
            try {
                jdbcTemplate = (JdbcTemplate) ctx.getBean("jdbcTemplate");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return jdbcTemplate;
    }

    public static DataSource getJdbcDataSource() {

        JdbcTemplate template = getJdbcTemplate();
        if (template == null) return null;

        return template.getDataSource();
    }

    public static void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        AppCtx.jdbcTemplate = jdbcTemplate;
    }

    public static StringRedisTemplate getStringRedisTemplate() {
        if (ctx != null && stringRedisTemplate == null) {
            try {
                stringRedisTemplate = (StringRedisTemplate) ctx.getBean("stringRedisTemplate");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return stringRedisTemplate;
    }

    public static void setRedisTemplate(StringRedisTemplate stringRedisTemplate) {
        AppCtx.stringRedisTemplate = stringRedisTemplate;
    }

    public static JedisConnectionFactory getJedisConnectionFactory() {

        StringRedisTemplate template = getStringRedisTemplate();
        if (template == null) return null;

        return (JedisConnectionFactory) stringRedisTemplate.getConnectionFactory();
    }

    public static GenericObjectPoolConfig getJedisPoolConfig() {

        JedisConnectionFactory factory = getJedisConnectionFactory();
        if (factory == null) return null;

        return factory.getPoolConfig();
    }
}
