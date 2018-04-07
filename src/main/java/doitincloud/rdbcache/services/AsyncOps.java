/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.services;

import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.commons.helpers.*;

import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import doitincloud.rdbcache.models.KvPair;

import javax.annotation.PostConstruct;

@Service
public class AsyncOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOps.class);

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
    }

    public void doSetExpKey(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSetExpKey: " + pairs.size() + " table: " + anyKey.printTable());

        // set expire key always runs asynchronously
        //
        Utils.getExcutorService().submit(() -> {

            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedis(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToRedis: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToDbase: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doUpdateToDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doUpateToDbase: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doPushOperations(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doPushOperations: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            AppCtx.getRedisRepo().update(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            AppCtx.getRedisRepo().update(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveAllToRedisAndSaveAllTodDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getRedisRepo().save(context, pairs,  anyKey);
            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs,  anyKey);
            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedisAndDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.printTable());

        if (context.isSync()) {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doPutOperation(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doPutOperation: " + pairs.size() + " table: " + anyKey.print());

        if (context.isSync()) {

            if (AppCtx.getRedisRepo().ifExist(context, pairs, anyKey)) {
                AppCtx.getRedisRepo().update(context, pairs, anyKey);
                AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            } else {
                AppCtx.getDbaseRepo().save(context, pairs, anyKey);
                AppCtx.getDbaseRepo().find(context, pairs, anyKey);
                AppCtx.getRedisRepo().save(context, pairs, anyKey);
            }
            doSetExpKey(context, pairs, anyKey);
            return;
        }

        Utils.getExcutorService().submit(() -> {

            if (AppCtx.getRedisRepo().ifExist(context, pairs, anyKey)) {
                AppCtx.getRedisRepo().update(context, pairs, anyKey);
                AppCtx.getDbaseRepo().update(context, pairs, anyKey);
            } else {
                AppCtx.getDbaseRepo().save(context, pairs, anyKey);
                AppCtx.getDbaseRepo().find(context, pairs, anyKey);
                AppCtx.getRedisRepo().save(context, pairs, anyKey);
            }
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doDeleteFromRedis(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doDeleteFromRedis: " + pairs.size());

        if (context.isSync()) {

            AppCtx.getRedisRepo().delete(context, pairs, anyKey);
            AppCtx.getKeyInfoRepo().delete(context, pairs);
            deleteKvPairsKeyInfo(context, pairs, anyKey);

            Utils.getExcutorService().submit(() -> {
                context.closeMonitor();
            });
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().delete(context, pairs, anyKey);
            AppCtx.getKeyInfoRepo().delete(context, pairs);
            deleteKvPairsKeyInfo(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doDeleteFromRedisAndDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doDeleteFromRedisAndDbase: " + pairs.size());

        if (context.isSync()) {

            AppCtx.getRedisRepo().delete(context, pairs, anyKey);
            AppCtx.getDbaseRepo().delete(context, pairs, anyKey);
            AppCtx.getKeyInfoRepo().delete(context, pairs);
            deleteKvPairsKeyInfo(context, pairs, anyKey);

            Utils.getExcutorService().submit(() -> {
                context.closeMonitor();
            });
            return;
        }

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().delete(context, pairs, anyKey);
            AppCtx.getDbaseRepo().delete(context, pairs, anyKey);
            AppCtx.getKeyInfoRepo().delete(context, pairs);
            deleteKvPairsKeyInfo(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void deleteKvPairKeyInfo(Context context, KvPair pair, KeyInfo keyInfo) {
        String kvType = "keyInfo";
        String type = pair.getType();
        if (!type.equals("data")) {
            kvType += ":" + type;
        }
        KvPair toDeletePair = new KvPair(pair.getId(), kvType);
        StopWatch stopWatch = context.startStopWatch("dbase", "KvPairRepo.delete");
        AppCtx.getKvPairRepo().delete(toDeletePair);
        if (stopWatch != null) stopWatch.stopNow();
    }

    public void deleteKvPairsKeyInfo(Context context, KvPairs pairs, AnyKey anyKey) {
        KvPairs toDeletePairs = new KvPairs();
        for (KvPair pair: pairs) {
            String kvType = "keyInfo";
            String type = pair.getType();
            if (!type.equals("data")) {
                kvType += ":" + type;
            }
            toDeletePairs.add(new KvPair(pair.getId(), kvType));
        }
        if (toDeletePairs.size() > 0) {
            StopWatch stopWatch = context.startStopWatch("dbase", "KvPairRepo.deleteAll");
            AppCtx.getKvPairRepo().deleteAll(toDeletePairs);
            if (stopWatch != null) stopWatch.stopNow();
        }
    }
}