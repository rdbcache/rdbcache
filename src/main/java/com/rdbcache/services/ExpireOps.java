/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.ScriptExecutor;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Collections;

@Service
public class ExpireOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpireOps.class);

    private String eventPrefix = Cfg.getEventPrefix();

    private Boolean enableMonitor = Cfg.getEnableMonitor();
    
    private Long eventLockTimeout = Cfg.getEventLockTimeout();

    @Autowired
    private StringRedisTemplate redisTemplate;

    private ValueOperations valueOps;

    private static DefaultRedisScript<Long> set_expire_key_script;

    private static DefaultRedisScript<String> expire_event_lock_script;

    private static DefaultRedisScript<Long> expire_event_unlock_script;

    private ScriptExecutor<String> scriptExecutor;

    @PostConstruct
    public void init() {

        valueOps = redisTemplate.opsForValue();

        set_expire_key_script = new DefaultRedisScript<>();
        set_expire_key_script.setLocation(new ClassPathResource("scripts/set-expire-key.lua"));
        set_expire_key_script.setResultType(Long.class);

        expire_event_lock_script = new DefaultRedisScript<>();
        expire_event_lock_script.setLocation(new ClassPathResource("scripts/expire-event-lock.lua"));
        expire_event_lock_script.setResultType(String.class);

        expire_event_unlock_script = new DefaultRedisScript<>();
        expire_event_unlock_script.setLocation(new ClassPathResource("scripts/expire-event-unlock.lua"));
        expire_event_unlock_script.setResultType(Long.class);

        scriptExecutor = new DefaultScriptExecutor<String>(redisTemplate);
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        eventPrefix = Cfg.getEventPrefix();
        enableMonitor = Cfg.getEnableMonitor();
        eventLockTimeout = Cfg.getEventLockTimeout();
    }

    public String getEventPrefix() {
        return eventPrefix;
    }

    public void setEventPrefix(String eventPrefix) {
        this.eventPrefix = eventPrefix;
    }

    public Boolean getEnableMonitor() {
        return enableMonitor;
    }

    public void setEnableMonitor(Boolean enableMonitor) {
        this.enableMonitor = enableMonitor;
    }

    public Long getEventLockTimeout() {
        return eventLockTimeout;
    }

    public void setEventLockTimeout(Long eventLockTimeout) {
        this.eventLockTimeout = eventLockTimeout;
    }

    // set up expire key
    //
    // expire = X,  it schedules an event in X seconds, only if not such event exists.
    //              The expiration event will happen at X seconds.
    //
    // expire = +X, it schedules an event in X seconds always, even if such event exists.
    //              It may use to delay the event, if next call happens before X seconds.
    //
    // expire = -X, it schedules a repeat event to occur every X seconds.
    //
    // expire = 0,  it removes existing event and not to set any event
    //
    public void setExpireKey(Context context, KeyInfo keyInfo) {

        KvPair pair = context.getPair();
        String key = pair.getId();
        //String table = keyInfo.getTable();

        LOGGER.trace("setExpireKey: " + key + " expire: " +keyInfo.getExpire());

        String expire = keyInfo.getExpire();
        String expKey = eventPrefix + "::" + key;

        StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
        Long result = scriptExecutor.execute(set_expire_key_script,
                Collections.singletonList(expKey), context.getTraceId(), expire);
        if (stopWatch != null) stopWatch.stopNow();

        if (result == 1 && keyInfo.getIsNew()) {
            AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);
        }
    }

    /**
     * To process key expired event
     *
     * @param event key expired event
     */
    public void onExpireEvent(String event) {

        LOGGER.trace("Received: " + event);

        if (!event.startsWith(eventPrefix)) {
            return;
        }

        String[] parts = event.split("::");

        if (parts.length < 3) {
            LOGGER.error("invalid event format");
            return;
        }

        String key = parts[1];
        String traceId = parts[2];

        Context context = new Context(key, traceId);
        if (enableMonitor) context.enableMonitor(event, "event", key);

        String lockKey = "lock_" + eventPrefix + "::" + key + "::" + traceId;
        String signature = Utils.generateId();

        StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
        String result = scriptExecutor.execute(expire_event_lock_script,
                Collections.singletonList(lockKey), signature, eventLockTimeout.toString());
        if (stopWatch != null) stopWatch.stopNow();

        if (!result.equals("OK")) {
            String msg = "unable to lock key: " + lockKey;
            LOGGER.trace(msg);
            context.stopFirstStopWatch();
            return;
        }

        try {

            KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
            if (keyInfo == null) {

                String msg = "keyInfo not found";
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                return;
            }

            LOGGER.trace(keyInfo.toString());

            Long expire = Long.valueOf(keyInfo.getExpire());

            if (expire > 0) {
                if (AppCtx.getRedisRepo().findOne(context, keyInfo)) {

                    AppCtx.getDbaseRepo().saveOne(context, keyInfo);
                    AppCtx.getRedisRepo().delete(context, keyInfo);
                    AppCtx.getKeyInfoRepo().deleteOne(context);

                } else {
                    String msg = "failed to find key from redis for " + key;
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                }
            }

            if (expire < 0) {
                if (AppCtx.getDbaseRepo().findOne(context, keyInfo)) {

                    AppCtx.getRedisRepo().saveOne(context, keyInfo);
                    setExpireKey(context, keyInfo);

                } else {
                    String msg = "failed to find key from database for " + key;
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            context.logTraceMessage(msg);

        } finally {

            stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
            scriptExecutor.execute(expire_event_unlock_script, Collections.singletonList(lockKey), signature);
            if (stopWatch != null) stopWatch.stopNow();

            context.stopFirstStopWatch();
        }
    }
}
