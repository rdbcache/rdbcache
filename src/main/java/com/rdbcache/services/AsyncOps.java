/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.Utils;

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

    @PostConstruct
    public void init() {

    }

    public void doSetExpKey(Context context, KeyInfo keyInfo) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSetExpKey: " + pairs.size() + " table: " + keyInfo.getTable());

        Utils.getExcutorService().submit(() -> {

            if (pairs.size() == 1) {
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
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

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, keyInfo);
            if (pairs.size() == 1) {
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
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

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().save(context, keyInfo);
            if (pairs.size() == 1) {
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
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

        Utils.getExcutorService().submit(() -> {

            List<KeyInfo> keyInfos = new ArrayList<>();

            if (AppCtx.getKeyInfoRepo().find(context, keyInfos)) {
                int i = 0;
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
                    KeyInfo keyInfoPer = keyInfos.get(i++);
                    AppCtx.getDbaseRepo().update(ctx, keyInfoPer);
                    AppCtx.getRedisRepo().save(ctx, keyInfoPer);
                    AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                }
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

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, keyInfo);
            AppCtx.getDbaseRepo().insert(context, keyInfo);
            for (KvPair pair : pairs) {
                Context ctx = context.getCopyWith(pair);
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

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, keyInfo);
            AppCtx.getDbaseRepo().save(context, keyInfo);

            if (pairs.size() == 1) {
                AppCtx.getExpireOps().setExpireKey(context, keyInfo);
            } else {
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
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

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair : pairs) {

                try {
                    Context ctx = context.getCopyWith(pair);
                    if (AppCtx.getRedisRepo().updateIfExists(ctx, keyInfo)) {
                        AppCtx.getDbaseRepo().save(ctx, keyInfo);
                        AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                        continue;
                    }

                    Context dbCtx = ctx.getCopyWith(pair.getId());
                    if (!AppCtx.getDbaseRepo().find(dbCtx, keyInfo)) {

                        AppCtx.getRedisRepo().save(ctx, keyInfo);
                        AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);

                    } else {

                        KvPair dbPair = dbCtx.getPair();
                        Map<String, Object> dbMap = dbPair.getData();            // loaded from database
                        Map<String, Object> map = pair.getData();                // input

                        if (keyInfo.getTable() != null) {

                            Map<String, Object> todoMap = new LinkedHashMap<String, Object>();

                            if (!Utils.mapChangesAfterUpdate(map, dbMap, todoMap)) {
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

                        if (!AppCtx.getRedisRepo().save(ctx, keyInfo)) {
                            LOGGER.error("failed to save to redis");
                            continue;
                        }

                        AppCtx.getKeyInfoRepo().save(ctx, keyInfo);
                        AppCtx.getDbaseRepo().save(ctx, keyInfo);
                        AppCtx.getExpireOps().setExpireKey(ctx, keyInfo);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair : pairs) {
                try {
                    Context ctx = context.getCopyWith(pair);
                    KeyInfo keyInfo = new KeyInfo();
                    if (!AppCtx.getKeyInfoRepo().find(ctx, keyInfo)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().deleteCompletely(ctx, keyInfo);
                } catch (Exception e) {
                    e.printStackTrace();
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

        Utils.getExcutorService().submit(() -> {

            for (int i = 0; i < pairs.size(); i++) {
                try {
                    KvPair pair = pairs.get(i);
                    System.out.println("key = " + pair.getId());
                    Context ctx = context.getCopyWith(pair);
                    KeyInfo keyInfo = new KeyInfo();
                    if (!AppCtx.getKeyInfoRepo().find(ctx, keyInfo)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().deleteCompletely(ctx, keyInfo);
                    AppCtx.getDbaseRepo().delete(ctx, keyInfo);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            context.closeMonitor();
        });
    }

}