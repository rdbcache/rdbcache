/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.Utils;
import com.rdbcache.models.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class AsyncOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOps.class);

    private static ExecutorService executor;

    @PostConstruct
    public void init() {
        executor = Executors.newCachedThreadPool();
    }

    public static ExecutorService getExecutor() {
        return executor;
    }
    
    public void doSetExpKey(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSetExpKey: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
            }
            context.closeMonitor();
        });
    }

    public void doSaveToRedis(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedis: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                AppCtx.getRedisRepo().saveOne(context, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                AppCtx.getRedisRepo().saveAll(context, keyInfo);
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
            }
            context.closeMonitor();
        });
    }

    public void doSaveToDbase(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToDbase: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                AppCtx.getDbaseRepo().saveOne(context, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                AppCtx.getDbaseRepo().saveAll(context, keyInfo);
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
            }
            context.closeMonitor();
        });
    }

    public void doPushOperations(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doPushOperations: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            List<KeyInfo> keyInfos = AppCtx.getKeyInfoRepo().findAll(context);
            int i = 0;
            for (KvPair pair : pairs) {
                Context ctx = context.getCloneWith(pair);
                KeyInfo keyInfoPer = keyInfos.get(i++);
                AppCtx.getDbaseRepo().updateOne(ctx, keyInfoPer);
                AppCtx.getRedisRepo().saveOne(ctx, keyInfoPer);
                AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
            }
            context.closeMonitor();
        });
    }

    public void doSaveAllToRedisAnInsertAllTodDbase(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            AppCtx.getDbaseRepo().insertAll(context, keyInfo);
            AppCtx.getRedisRepo().saveAll(context, keyInfo);
            for (KvPair pair : pairs) {
                Context ctx = context.getCloneWith(pair);
                AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
            }
            context.closeMonitor();
        });
    }

    public void doSaveToRedisAndDbase(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                AppCtx.getDbaseRepo().saveOne(context, keyInfo);
                AppCtx.getRedisRepo().saveOne(context, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                AppCtx.getDbaseRepo().saveAll(context, keyInfo);
                AppCtx.getRedisRepo().saveAll(context, keyInfo);
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
            }
            context.closeMonitor();
        });
    }

    public void doPutOperation(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doPutOperation: " + pairs.size() + " table: " + keyInfo.getTable());

        executor.submit(() -> {

            Thread.yield();

            for (KvPair pair : pairs) {

                Context ctx = context.getCloneWith(pair);
                if (AppCtx.getRedisRepo().updateIfExists(ctx, keyInfo)) {
                    AppCtx.getDbaseRepo().saveOne(ctx, keyInfo);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                    continue;
                }

                Context dbCtx = ctx.getCloneWith(pair.getId());
                if (!AppCtx.getDbaseRepo().findOne(dbCtx, keyInfo)) {

                    AppCtx.getRedisRepo().saveOne(ctx, keyInfo);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);

                } else {

                    KvPair dbPair = dbCtx.getPair();
                    Map<String, Object> dbMap = dbPair.getData();            // loaded from database
                    Map<String, Object> map = pair.getData();                // input

                    if (keyInfo.getTable() != null) {

                        Map<String, Object> todoMap = new LinkedHashMap<String, Object>();

                        if (!Utils.MapChangesAfterUpdate(map, dbMap, todoMap)) {
                            LOGGER.error("field found in input, but not found in database");
                            continue;
                        }
                        for (Map.Entry<String, Object> entry : todoMap.entrySet()) {
                            dbMap.put(entry.getKey(), entry.getValue());     // overwrite dbMap
                        }
                    } else if (!Utils.isMapEquals(map, dbMap)) {

                        for (Map.Entry<String, Object> entry : map.entrySet()) {
                            dbMap.put(entry.getKey(), entry.getValue());     // overwrite dbMap
                        }
                    } else {
                        AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                        continue;
                    }

                    pair.setData(dbMap);

                    if (!AppCtx.getRedisRepo().saveOne(ctx, keyInfo)) {
                        LOGGER.error("failed to save to redis");
                        continue;
                    }

                    AppCtx.getKeyInfoRepo().saveOne(ctx, keyInfo);
                    AppCtx.getDbaseRepo().saveOne(ctx, keyInfo);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
            }
            context.closeMonitor();
        });
    }

    public void doDeleteFromRedis(Context context) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doDeleteFromRedis: " + pairs.size());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
                if (keyInfo == null) {
                    context.logTraceMessage("key not found");
                } else {
                    AppCtx.getRedisRepo().deleteOneCompletely(context, keyInfo);
                }
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(ctx);
                    if (keyInfo == null) {
                        context.logTraceMessage("key (" + pair.getId() + ") not found");
                    } else {
                        AppCtx.getRedisRepo().deleteOneCompletely(ctx, keyInfo);
                    }
                }
            }
            context.closeMonitor();
        });
    }

    public void doDeleteFromRedisAndDbase(Context context) {
        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doDeleteFromRedisAndDbase: " + pairs.size());

        executor.submit(() -> {

            Thread.yield();

            if (pairs.size() == 1) {
                KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
                if (keyInfo == null) {
                    context.logTraceMessage("key not found");
                } else {
                    AppCtx.getRedisRepo().deleteOneCompletely(context, keyInfo);
                    AppCtx.getDbaseRepo().deleteOne(context, keyInfo);
                }
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCloneWith(pair);
                    KeyInfo keyInfo = AppCtx.getKeyInfoRepo().findOne(ctx);
                    if (keyInfo == null) {
                        context.logTraceMessage("key (" + pair.getId() + ") not found");
                    } else {
                        AppCtx.getRedisRepo().deleteOneCompletely(ctx, keyInfo);
                        AppCtx.getDbaseRepo().deleteOne(ctx, keyInfo);
                    }
                }
            }
            context.closeMonitor();
        });
    }

}