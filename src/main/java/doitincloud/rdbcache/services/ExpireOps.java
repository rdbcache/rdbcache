/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.helpers.*;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.controllers.RdbcacheApis;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.models.StopWatch;
import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.ExpireDbOps;
import doitincloud.rdbcache.supports.KvPairs;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
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

    private String eventPrefix = PropCfg.getEventPrefix();

    private Boolean enableMonitor = PropCfg.getEnableMonitor();
    
    private Long eventLockTimeout = PropCfg.getEventLockTimeout();

    private ValueOperations valueOps;

    private static DefaultRedisScript<Long> set_expire_key_script;

    private static DefaultRedisScript<String> expire_event_lock_script;

    private static DefaultRedisScript<Long> expire_event_unlock_script;

    private ScriptExecutor<String> scriptExecutor;

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        eventPrefix = PropCfg.getEventPrefix();
        enableMonitor = PropCfg.getEnableMonitor();
        eventLockTimeout = PropCfg.getEventLockTimeout();
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {

        StringRedisTemplate stringRedisTemplate = AppCtx.getStringRedisTemplate();
        if (stringRedisTemplate == null) {
            LOGGER.error("failed to get redis template");
            return;
        }
        valueOps = stringRedisTemplate.opsForValue();
        // setup for test
        if (valueOps == null || "__TRUE__".equals(valueOps.get("__is_mock_test__"))) {
            return;
        }

        set_expire_key_script = new DefaultRedisScript<>();
        set_expire_key_script.setLocation(new ClassPathResource("scripts/set-expire-key.lua"));
        set_expire_key_script.setResultType(Long.class);

        expire_event_lock_script = new DefaultRedisScript<>();
        expire_event_lock_script.setLocation(new ClassPathResource("scripts/expire-event-lock.lua"));
        expire_event_lock_script.setResultType(String.class);

        expire_event_unlock_script = new DefaultRedisScript<>();
        expire_event_unlock_script.setLocation(new ClassPathResource("scripts/expire-event-unlock.lua"));
        expire_event_unlock_script.setResultType(Long.class);

        scriptExecutor = new DefaultScriptExecutor<String>(stringRedisTemplate);
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
    public void setExpireKey(Context context, KvPair pair, KeyInfo keyInfo) {

        try {
            String key = pair.getId();
            String type = pair.getType();

            LOGGER.trace("setExpireKey: " + pair.printKey() + " expire: " + keyInfo.getExpire());

            String expire = keyInfo.getExpire();
            String expKey = eventPrefix + "::" + type + ":" + key;

            StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
            Long result = scriptExecutor.execute(set_expire_key_script,
                    Collections.singletonList(expKey), context.getTraceId(), expire);
            if (stopWatch != null) stopWatch.stopNow();

            if (result != 1) {
                keyInfo.restoreExpire();
            }
            if (keyInfo.getIsNew()) {
                AppCtx.getKeyInfoRepo().save(context, pair, keyInfo);
            }
        } catch (Exception e) {
            String msg = e.getCause().getMessage();
            LOGGER.error(msg);
            context.logTraceMessage(msg);
        }
    }

    public void setExpireKey(Context context, KvPairs pairs, AnyKey anyKey) {

        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            setExpireKey(context, pair, keyInfo);
        }
    }

    /**
     * To process key expired event
     *
     * @param event key expired event
     */
    public void onExpireEvent(String event) {

        LOGGER.debug("Received: " + event);

        if (!event.startsWith(eventPrefix)) {
            return;
        }

        String[] parts = event.split("::");

        if (parts.length < 3) {
            LOGGER.error("invalid event format");
            return;
        }

        String hashKey = parts[1];
        int index = hashKey.indexOf(":");
        if (index < 0) {
            LOGGER.error("invalid event format, failed to figure out type and key");
            return;
        }
        String type = hashKey.substring(0, index);
        String key = hashKey.substring(index+1);

        String traceId = parts[2];

        Context context = new Context(traceId);
        KvPair pair = new KvPair(key, type);

        if (enableMonitor) context.enableMonitor(event, "event", key);

        String lockKey = "lock_" + eventPrefix + "::" + hashKey + "::" + traceId;
        String signature = Utils.generateId();

        StopWatch stopWatch = context.startStopWatch("redis", "scriptExecutor.execute");
        String result = scriptExecutor.execute(expire_event_lock_script,
                Collections.singletonList(lockKey), signature, eventLockTimeout.toString());
        if (stopWatch != null) stopWatch.stopNow();

        if (!result.equals("OK")) {
            String msg = "unable to lock key: " + lockKey;
            LOGGER.trace(msg);
            context.closeMonitor();
            return;
        }

        try {

            KvPairs pairs = new KvPairs(pair);
            AnyKey anyKey = new AnyKey();

            if (!AppCtx.getKeyInfoRepo().find(context, pairs, anyKey)) {

                String msg = "keyInfo not found";
                LOGGER.error(msg);
                context.logTraceMessage(msg);
                return;
            }

            KeyInfo keyInfo = anyKey.getKeyInfo();

            LOGGER.trace(keyInfo.toString());

            Long expire = Long.valueOf(keyInfo.getExpire());

            if (expire > 0) {
                if (AppCtx.getRedisRepo().find(context, pairs, anyKey)) {

                    String qkey = keyInfo.getQueryKey();
                    if (qkey == null || !qkey.equals("NOOPS")) {
                        AppCtx.getDbaseRepo().save(context, pairs, anyKey);
                    } else if (qkey != null && qkey.startsWith("ExpireDbOps::")) {
                        String beanName = qkey.substring(13);
                        ApplicationContext ctx = AppCtx.getApplicationContext();
                        if (ctx != null) {
                            try {
                                ExpireDbOps ops = (ExpireDbOps) ctx.getBean(beanName);
                                if (ops != null) {
                                    ops.save(context, pairs, anyKey);
                                } else {
                                    LOGGER.error("failed to get bean: " + beanName);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } else {
                        LOGGER.trace("queryKey = " + keyInfo.getQueryKey());
                    }
                    AppCtx.getRedisRepo().delete(context, pairs, anyKey);
                    AppCtx.getKeyInfoRepo().delete(context, pairs);

                } else {
                    String msg = "failed to find key from redis for " + key;
                    LOGGER.error(msg);
                    context.logTraceMessage(msg);
                }
            }

            if (expire < 0) {
                if (AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

                    AppCtx.getRedisRepo().save(context, pairs, anyKey);
                    setExpireKey(context, pairs, anyKey);

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

            context.closeMonitor();
        }
    }
}
