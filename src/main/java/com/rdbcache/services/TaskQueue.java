/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.models.KeyInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Service
public class TaskQueue extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueue.class);

    private Boolean enableMonitor = Cfg.getEnableMonitor();

    private String queueName = Cfg.getQueueName();

    private ListOperations listOps;

    @Autowired
    StringRedisTemplate redisTemplate;

    @PostConstruct
    public void init() {
        listOps = redisTemplate.opsForList();
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        enableMonitor = Cfg.getEnableMonitor();
        queueName = Cfg.getQueueName();
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
        start();
    }

    public Boolean getEnableMonitor() {
        return enableMonitor;
    }

    public void setEnableMonitor(Boolean enableMonitor) {
        this.enableMonitor = enableMonitor;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    private boolean isRunnning = true;

    public boolean isRunnning() {
        return isRunnning;
    }

    @Override
    public void interrupt() {
        isRunnning = false;
        super.interrupt();
    }

    private boolean freshConnection = true;

    @Override
    public void run() {

        LOGGER.info("Task Queue is running on thread " + getName());

        while (isRunnning) {

            try {

                if (freshConnection) {
                    AppCtx.getRedisOps().ensureNotifyKeySpaceEventsEx();
                    freshConnection = false;
                }

                String task = (String) listOps.leftPop(queueName, 0, TimeUnit.SECONDS);

                if (!isRunnning) break;

                LOGGER.info("Received Task: " + task);

                String[] parts = task.split("::");

                if (parts.length < 3) {
                    LOGGER.error("invalid task format");
                    return;
                }

                String action = parts[0];
                String key = parts[1];
                String traceId = parts[2];

                Context context = new Context(key, traceId);

                if (enableMonitor) context.enableMonitor(task, "queue", action);

                KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
                if (keyInfo == null) {
                    keyInfo = new KeyInfo(Cfg.getDefaultExpire(), "value");
                }

                //...

                String msg = "unknown task action:" + action;
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                context.stopFirstStopWatch();

            } catch (RedisConnectionFailureException e) {

                LOGGER.warn("Connection failure occurred. Restarting task queue after 5000 ms");

                e.printStackTrace();

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }

                freshConnection = true;

            } catch (Exception e) {
                String msg = e.getCause().getMessage();
                LOGGER.error(msg);
                e.printStackTrace();
            }
        }

        isRunnning = false;

    }
}
