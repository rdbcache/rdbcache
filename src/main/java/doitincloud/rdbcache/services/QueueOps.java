/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.KvPairs;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
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
public class QueueOps extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueOps.class);

    private Boolean enableMonitor = PropCfg.getEnableMonitor();

    private String queueName = PropCfg.getQueueName();

    private ListOperations listOps;

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        enableMonitor = PropCfg.getEnableMonitor();
        queueName = PropCfg.getQueueName();
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {

        StringRedisTemplate stringRedisTemplate = AppCtx.getStringRedisTemplate();
        if (stringRedisTemplate == null) {
            LOGGER.error("failed to get redis template");
            return;
        }
        listOps = stringRedisTemplate.opsForList();
        // setup for test
        if (listOps == null) {
            return;
        }
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

    private boolean isRunning = false;

    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void interrupt() {
        isRunning = false;
        super.interrupt();
    }

    private boolean freshConnection = true;

    @Override
    public void run() {

        isRunning = true;

        LOGGER.debug("QueueOps is running on thread " + getName());

        while (isRunning) {

            try {

                if (freshConnection) {
                    AppCtx.getRedisOps().ensureNotifyKeySpaceEventsEx();
                    freshConnection = false;
                }

                String task = (String) listOps.leftPop(queueName, 0, TimeUnit.SECONDS);

                if (!isRunning) break;

                if (task == null) continue;
                
                onReceiveTask(task);

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

        isRunning = false;

    }

    public void onReceiveTask(String task) {

        LOGGER.debug("Received Task: " + task);

        String[] parts = task.split("::");

        if (parts.length < 3) {
            LOGGER.error("invalid task format");
            return;
        }

        String action = parts[0];
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
        if (enableMonitor) context.enableMonitor(task, "queue", action);
        KvPair pair = new KvPair(key, type);

        KvPairs pairs = new KvPairs(pair);
        AnyKey anyKey = new AnyKey();
        if (!AppCtx.getKeyInfoRepo().find(context, pairs, anyKey)) {
            String msg = "keyInfo not found";
            LOGGER.error(msg);
            context.logTraceMessage(msg);
            return;
        }

        KeyInfo keyInfo = anyKey.getKeyInfo();

        //...

        String msg = "unknown task action:" + action;
        LOGGER.error(msg);
        context.logTraceMessage(msg);
        context.closeMonitor();
    }
}
