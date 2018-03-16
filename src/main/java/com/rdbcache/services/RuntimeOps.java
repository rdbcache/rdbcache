/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

@Service
@Profile({"dev", "test"})
public class RuntimeOps extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeOps.class);

    private Properties properties = new Properties();

    @PostConstruct
    public void init() {
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @EventListener
    public void handleRefreshedEvent(ContextRefreshedEvent event) {
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {

        assertCfgProperties();

        assertCfgPerBean();

        start();
    }

    private boolean isRunning = true;

    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void interrupt() {
        isRunning = false;
        super.interrupt();
    }

    @Override
    public void run() {

        LOGGER.debug("RuntimeOps is running on thread " + getName());

        while (isRunning) {

            try {

                try {
                    Thread.sleep(60000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!isRunning) break;

                LOGGER.debug("RuntimeOps thread starts to check ...");

                Assert.isTrue(AppCtx.getLocalCache().isAlive(), "LocalCache is not alive");
                Assert.isTrue(!AppCtx.getLocalCache().getState().name().equals("TERMINATED"), "LocalCache is terminated");
                Assert.isTrue(AppCtx.getLocalCache().isRunning(), "LocalCache is not running");

                Assert.isTrue(AppCtx.getTaskQueue().isAlive(), "TaskQueue is not alive");
                Assert.isTrue(!AppCtx.getTaskQueue().getState().name().equals("TERMINATED"), "TaskQueue is terminated");
                Assert.isTrue(AppCtx.getTaskQueue().isRunning(), "TaskQueue is not running");

            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }
        }

        isRunning = false;
    }

    private void assertCfgProperties() {

        Assert.isTrue(properties.getProperty("rdbcache.hdata_prefix").equals(
                PropCfg.getHdataPrefix()), "PropCfg.getHdataPrefix() not match");

        Assert.isTrue(properties.getProperty("rdbcache.hkeys_prefix").equals(
                PropCfg.getHkeyPrefix()), "PropCfg.getHkeyPrefix() not match");

        Assert.isTrue(properties.getProperty("rdbcache.event_prefix").equals(
                PropCfg.getEventPrefix()), "PropCfg.getEventPrefix() not match");

        Assert.isTrue(properties.getProperty("rdbcache.queue_name").equals(
                PropCfg.getQueueName()), "PropCfg.getQueueName() not match");

        Assert.isTrue(properties.getProperty("rdbcache.default_expire").equals(
                PropCfg.getDefaultExpire()), "PropCfg.getDefaultExpire() not match");

        Assert.isTrue(properties.getProperty("rdbcache.enable_monitor").equals(
                PropCfg.getEnableMonitor().toString()), "PropCfg.getEnableMonitor() not match");

        Assert.isTrue(properties.getProperty("rdbcache.event_lock_timeout").equals(
                PropCfg.getEventLockTimeout().toString()), "PropCfg.getEventLockTimeout() not match");

        Assert.isTrue(properties.getProperty("rdbcache.key_min_cache_ttl").equals(
                PropCfg.getKeyMinCacheTTL().toString()), "PropCfg.getKeyMinCacheTTL() not match");

        Assert.isTrue(properties.getProperty("rdbcache.table_info_cache_ttl").equals(
                PropCfg.getTableInfoCacheTTL().toString()), "PropCfg.getTableInfoCacheTTL() not match");

        Assert.isTrue(properties.getProperty("rdbcache.local_cache_max_size").equals(
                PropCfg.getMaxCacheSize().toString()), "PropCfg.getMaxCacheSize() not match");

        Assert.isTrue(properties.getProperty("rdbcache.cache_recycle_secs").equals(
                PropCfg.getCacheRecycleSecs().toString()), "PropCfg.getCacheRecycleSecs() not match");

        Assert.isTrue(properties.getProperty("rdbcache.enable_db_fallback").equals(
                PropCfg.getEnableDbFallback().toString()), "PropCfg.getEnableDbFallback() not match");

        Assert.isTrue(properties.getProperty("rdbcache.data_max_cache_ttl").equals(
                PropCfg.getDataMaxCacheTLL().toString()), "PropCfg.getDataMaxCacheTLL() not match");
    }

    private void assertCfgPerBean() {

        //TaskQueue
        Assert.isTrue(AppCtx.getTaskQueue().getEnableMonitor().toString().equals(
                properties.getProperty("rdbcache.enable_monitor")),
                "rdbcache.enable_monitor not match");
        Assert.isTrue(AppCtx.getTaskQueue().getQueueName().equals(
                properties.getProperty("rdbcache.queue_name")),
                "rdbcache.queue_name not match");

        //LocalCache
        Assert.isTrue(AppCtx.getLocalCache().getRecycleSecs().toString().equals(
                properties.getProperty("rdbcache.cache_recycle_secs")),
                "rdbcache.cache_recycle_secs not match");
        Assert.isTrue(AppCtx.getLocalCache().getDataMaxCacheTLL().toString().equals(
                properties.getProperty("rdbcache.data_max_cache_ttl")),
                "rdbcache.data_max_cache_ttl not match");
        Assert.isTrue(AppCtx.getLocalCache().getKeyMinCacheTTL().toString().equals(
                properties.getProperty("rdbcache.key_min_cache_ttl")),
                "rdbcache.key_min_cache_ttl not match");
        Assert.isTrue(AppCtx.getLocalCache().getMaxCacheSize().toString().equals(
                properties.getProperty("rdbcache.local_cache_max_size")),
                "rdbcache.local_cache_max_size not match");

        //ExpireOps
        Assert.isTrue(AppCtx.getExpireOps().getEnableMonitor().toString().equals(
                properties.getProperty("rdbcache.enable_monitor")),
                "rdbcache.enable_monitor not match");
        Assert.isTrue(AppCtx.getExpireOps().getEventLockTimeout().toString().equals(
                properties.getProperty("rdbcache.event_lock_timeout")),
                "rdbcache.event_lock_timeout not match");
        Assert.isTrue(AppCtx.getExpireOps().getEventPrefix().equals(
                properties.getProperty("rdbcache.event_prefix")),
                "rdbcache.event_prefix not match");

        //DbaseOps
        Assert.isTrue(AppCtx.getDbaseOps().getTableInfoCacheTTL().toString().equals(
                properties.getProperty("rdbcache.table_info_cache_ttl")),
                "rdbcache.table_info_cache_ttl not match");

        //DbaseRepoImpl
        Assert.isTrue(AppCtx.getDbaseRepoImpl().getEnableDbFallback().toString().equals(
                properties.getProperty("rdbcache.enable_db_fallback")),
                "rdbcache.enable_db_fallback not match");

        //KeyInfoRepoImpl
        Assert.isTrue(AppCtx.getKeyInfoRepoImpl().getHkeyPrefix().equals(
                properties.getProperty("rdbcache.hkeys_prefix")),
                "rdbcache.hkeys_prefix not match");

        //RedisRepoImpl
        Assert.isTrue(AppCtx.getRedisRepoImpl().getEventPrefix().equals(
                properties.getProperty("rdbcache.event_prefix")),
                "rdbcache.event_prefix not match");
        Assert.isTrue(AppCtx.getRedisRepoImpl().getHdataPrefix().equals(
                properties.getProperty("rdbcache.hdata_prefix")),
                "rdbcache.hdata_prefix not match");
    }
}
