/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.controllers.RdbcacheApis;
import com.rdbcache.repositories.*;
import com.rdbcache.repositories.impls.DbaseRepoImpl;
import com.rdbcache.repositories.impls.KeyInfoRepoImpl;
import com.rdbcache.repositories.impls.RedisRepoImpl;
import com.rdbcache.services.*;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

public class AppCtx {

    private static ApplicationContext ctx;

    private static RdbcacheApis rdbcacheApis;

    private static AsyncOps asyncOps;

    private static DbaseOps dbaseOps;

    private static ExpireOps expireOps;

    private static LocalCache localCache;

    private static RedisOps redisOps;

    private static TaskQueue taskQueue;

    private static DbaseRepo dbaseRepo;

    private static KeyInfoRepo keyInfoRepo;

    private static KvPairRepo kvPairRepo;

    private static MonitorRepo monitorRepo;

    private static RedisRepo redisRepo;

    private static StopWatchRepo stopWatchRepo;

    private static JdbcTemplate jdbcTemplate;

    private static StringRedisTemplate redisTemplate;

    public static void setApplicationContext(ApplicationContext ctx) {
        AppCtx.ctx = ctx;
    }

    public static ApplicationContext getApplicationContext() {
        return ctx;
    }

    public static RdbcacheApis getRdbcacheApis() {
        if (rdbcacheApis == null) {
            try {
                rdbcacheApis = ctx.getBean(RdbcacheApis.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rdbcacheApis;
    }

    public static AsyncOps getAsyncOps() {
        if (asyncOps == null) {
            try {
                asyncOps = ctx.getBean(AsyncOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return asyncOps;
    }

    public static DbaseOps getDbaseOps() {
        if (dbaseOps == null) {
            try {
                dbaseOps = ctx.getBean(DbaseOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dbaseOps;
    }

    public static ExpireOps getExpireOps() {
        if (expireOps == null) {
            try {
                expireOps = ctx.getBean(ExpireOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return expireOps;
    }

    public static RedisOps getRedisOps() {
        if (redisOps == null) {
            try {
                redisOps = ctx.getBean(RedisOps.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisOps;
    }

    public static LocalCache getLocalCache() {
        if (localCache == null) {
            try {
                localCache = ctx.getBean(LocalCache.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return localCache;
    }

    public static TaskQueue getTaskQueue() {
        if (taskQueue == null) {
            try {
                taskQueue = ctx.getBean(TaskQueue.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return taskQueue;
    }

    public static DbaseRepo getDbaseRepo() {
        if (dbaseRepo == null) {
            try {
                dbaseRepo = ctx.getBean(DbaseRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dbaseRepo;
    }

    public static DbaseRepoImpl getDbaseRepoImpl() {
        try {
            return ctx.getBean(DbaseRepoImpl.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static KeyInfoRepo getKeyInfoRepo() {
        if (keyInfoRepo == null) {
            try {
                keyInfoRepo = ctx.getBean(KeyInfoRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return keyInfoRepo;
    }

    public static KeyInfoRepoImpl getKeyInfoRepoImpl() {
        try {
            return ctx.getBean(KeyInfoRepoImpl.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static KvPairRepo getKvPairRepo() {
        if (kvPairRepo == null) {
            try {
                kvPairRepo = ctx.getBean(KvPairRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return kvPairRepo;
    }

    public static MonitorRepo getMonitorRepo() {
        if (monitorRepo == null) {
            try {
                monitorRepo = ctx.getBean(MonitorRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return monitorRepo;
    }

    public static RedisRepo getRedisRepo() {
        if (redisRepo == null) {
            try {
                redisRepo = ctx.getBean(RedisRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisRepo;
    }

    public static RedisRepoImpl getRedisRepoImpl() {
        try {
            return ctx.getBean(RedisRepoImpl.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static StopWatchRepo getStopWatchRepo() {
        if (stopWatchRepo == null) {
            try {
                stopWatchRepo = ctx.getBean(StopWatchRepo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return stopWatchRepo;
    }

    public static JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            try {
                jdbcTemplate = (JdbcTemplate) ctx.getBean("jdbcTemplate");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return jdbcTemplate;
    }

    public static StringRedisTemplate getRedisTemplate() {
        if (redisTemplate == null) {
            try {
                redisTemplate = (StringRedisTemplate) ctx.getBean("redisTemplate");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisTemplate;
    }
}
