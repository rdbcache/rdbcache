/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
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
        String expKey = Cfg.getEventPrefix() + "::" + key;

        StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
        Long result = scriptExecutor.execute(set_expire_key_script,
                Collections.singletonList(expKey), context.getTraceId(), expire);
        if (stopWatch != null) stopWatch.stopNow();

        if (result == 1 && keyInfo.getIsNew()) {
            AppCtx.getKeyInfoRepo().saveOne(context, keyInfo);
        }
    }

    /**
     * To process key expired message
     *
     * @param message key expired message
     */
    public void onMessage(String message) {

        LOGGER.trace("Received message: " + message);

        if (!message.startsWith(Cfg.getEventPrefix())) {
            return;
        }

        String[] parts = message.split("::");

        if (parts.length < 3) {
            LOGGER.error("invalid message format");
            return;
        }

        String key = parts[1];
        String traceId = parts[2];

        Context context = new Context(key, traceId);
        if (Cfg.getEnableMonitor()) context.enableMonitor(message, "event", key);

        String lockKey = "lock_" + Cfg.getEventPrefix() + "::" + key + "::" + traceId;
        String signature = Utils.generateId();

        StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
        String result = scriptExecutor.execute(expire_event_lock_script,
                Collections.singletonList(lockKey), signature, Cfg.getEventLockTimeout().toString());
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
