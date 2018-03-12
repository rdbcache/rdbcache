/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
public class AsyncOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOps.class);

    @PostConstruct
    public void init() {

    }

    public void doSetExpKey(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSetExpKey: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getExpireOps().setExpireKey(context, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedis(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedis: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToDbase(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().save(context, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, anyKey);
            context.closeMonitor();
        });
    }

    public void doPushOperations(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doPushOperations: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AnyKey anyKeyNew = new AnyKey();
            //List<KeyInfo> keyInfos = new ArrayList<>();
            if (AppCtx.getKeyInfoRepo().find(context, anyKeyNew)) {
                int i = 0;
                for (KvPair pair : pairs) {
                    Context ctx = context.getCopyWith(pair);
                    KeyInfo keyInfo = anyKeyNew.getAny(i++);
                    AppCtx.getDbaseRepo().update(ctx, new AnyKey(keyInfo));
                    AppCtx.getRedisRepo().save(ctx, new AnyKey(keyInfo));
                }
                AppCtx.getExpireOps().setExpireKey(context, anyKey);
            }
            context.closeMonitor();
        });
    }

    public void doSaveAllToRedisAnInsertAllTodDbase(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, anyKey);
            AppCtx.getDbaseRepo().insert(context, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedisAndDbase(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, anyKey);
            AppCtx.getDbaseRepo().save(context, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, anyKey);
            context.closeMonitor();
        });
    }

    public void doPutOperation(Context context, AnyKey anyKey) {

        List<KvPair> pairs = context.getPairs();
        if (pairs == null || pairs.size() == 0) {
            return;
        }

        LOGGER.trace("doPutOperation: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair : pairs) {

                try {
                    Context ctx = context.getCopyWith(pair);
                    if (AppCtx.getRedisRepo().updateIfExists(ctx, anyKey)) {
                        AppCtx.getDbaseRepo().save(ctx, anyKey);
                        AppCtx.getExpireOps().setExpireKey(ctx, anyKey);
                        continue;
                    }

                    Context dbCtx = ctx.getCopyWith(pair.getId());
                    if (!AppCtx.getDbaseRepo().find(dbCtx, anyKey)) {

                        AppCtx.getRedisRepo().save(ctx, anyKey);
                        AppCtx.getExpireOps().setExpireKey(ctx, anyKey);

                    } else {

                        KvPair dbPair = dbCtx.getPair();
                        Map<String, Object> dbMap = dbPair.getData();            // loaded from database
                        Map<String, Object> map = pair.getData();                // input

                        if (anyKey.getKey().getTable() != null) {

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
                            AppCtx.getExpireOps().setExpireKey(ctx, anyKey);
                            continue;
                        }

                        pair.setData(dbMap);

                        if (!AppCtx.getRedisRepo().save(ctx, anyKey)) {
                            LOGGER.error("failed to save to redis");
                            continue;
                        }

                        AppCtx.getKeyInfoRepo().save(ctx, anyKey);
                        AppCtx.getDbaseRepo().save(ctx, anyKey);
                        AppCtx.getExpireOps().setExpireKey(ctx, anyKey);
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
                    AnyKey anyKey = new AnyKey(new KeyInfo());
                    anyKey.getKey().setIsNew(false);
                    if (!AppCtx.getKeyInfoRepo().find(ctx, anyKey)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().deleteCompletely(ctx, anyKey);
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
                    AnyKey anyKey = new AnyKey(new KeyInfo());
                    anyKey.getKey().setIsNew(false);
                    if (!AppCtx.getKeyInfoRepo().find(ctx, anyKey)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().deleteCompletely(ctx, anyKey);
                    AppCtx.getDbaseRepo().delete(ctx, anyKey);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            context.closeMonitor();
        });
    }

}