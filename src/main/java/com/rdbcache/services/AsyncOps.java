/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.services;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
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

    public void doSetExpKey(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSetExpKey: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedis(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToRedis: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doPushOperations(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doPushOperations: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AnyKey anyKeyNew = new AnyKey();
            //List<KeyInfo> keyInfos = new ArrayList<>();
            if (AppCtx.getKeyInfoRepo().find(context, pairs, anyKeyNew)) {
                int i = 0;
                for (KvPair pair : pairs) {
                    KvPairs kvpPairs = new KvPairs(pair);
                    AnyKey akey = anyKeyNew.getAnyKey(i++);
                    AppCtx.getDbaseRepo().update(context, kvpPairs, akey);
                    AppCtx.getRedisRepo().save(context, kvpPairs, akey);
                }
                AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            }
            context.closeMonitor();
        });
    }

    public void doSaveAllToRedisAnInsertAllTodDbase(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs,  anyKey);
            AppCtx.getDbaseRepo().insert(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doSaveToRedisAndDbase(Context context, KvPairs pairs, AnyKey anyKey) {


        LOGGER.trace("doSaveToRedisAndDbase: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            AppCtx.getRedisRepo().save(context, pairs, anyKey);
            AppCtx.getDbaseRepo().save(context, pairs, anyKey);
            AppCtx.getExpireOps().setExpireKey(context, pairs, anyKey);
            context.closeMonitor();
        });
    }

    public void doPutOperation(Context context, KvPairs pairs, AnyKey anyKey) {

        LOGGER.trace("doPutOperation: " + pairs.size() + " table: " + anyKey.getKey().getTable());

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair : pairs) {

                KvPairs kvPairs = new KvPairs(pair);

                try {

                    if (AppCtx.getRedisRepo().updateIfExists(context, kvPairs, anyKey)) {
                        AppCtx.getDbaseRepo().save(context, kvPairs, anyKey);
                        AppCtx.getExpireOps().setExpireKey(context, kvPairs, anyKey);
                        continue;
                    }

                    KvPairs dbPairs = new KvPairs(pair.getId());
                    if (!AppCtx.getDbaseRepo().find(context, dbPairs, anyKey)) {

                        AppCtx.getRedisRepo().save(context, kvPairs, anyKey);
                        AppCtx.getExpireOps().setExpireKey(context, kvPairs, anyKey);

                    } else {

                        KvPair dbPair = dbPairs.getPair();
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
                            AppCtx.getExpireOps().setExpireKey(context, kvPairs, anyKey);
                            continue;
                        }

                        pair.setData(dbMap);

                        if (!AppCtx.getRedisRepo().save(context, kvPairs, anyKey)) {
                            LOGGER.error("failed to save to redis");
                            continue;
                        }

                        AppCtx.getKeyInfoRepo().save(context, kvPairs, anyKey);
                        AppCtx.getDbaseRepo().save(context, kvPairs, anyKey);
                        AppCtx.getExpireOps().setExpireKey(context, kvPairs, anyKey);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            context.closeMonitor();
        });
    }

    public void doDeleteFromRedis(Context context, KvPairs pairs) {

        LOGGER.trace("doDeleteFromRedis: " + pairs.size());

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair : pairs) {

                KvPairs newPairs = new KvPairs(pair);
                AnyKey anyKey = new AnyKey(new KeyInfo());
                anyKey.getKey().setIsNew(false);

                try {
                    if (!AppCtx.getKeyInfoRepo().find(context, newPairs, anyKey)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().delete(context, newPairs, anyKey, false);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            context.closeMonitor();
        });
    }

    public void doDeleteFromRedisAndDbase(Context context, KvPairs pairs) {

        LOGGER.trace("doDeleteFromRedisAndDbase: " + pairs.size());

        Utils.getExcutorService().submit(() -> {

            for (KvPair pair: pairs) {

                KvPairs newPairs = new KvPairs(pair);
                AnyKey anyKey = new AnyKey(new KeyInfo());
                anyKey.getKey().setIsNew(false);

                try {
                    if (!AppCtx.getKeyInfoRepo().find(context, newPairs, anyKey)) {
                        String message = "key (" + pair.getId() + ") not found";
                        LOGGER.warn(message);
                        context.logTraceMessage(message);
                    }
                    AppCtx.getRedisRepo().delete(context, newPairs, anyKey, true);
                    AppCtx.getDbaseRepo().delete(context, newPairs, anyKey);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            context.closeMonitor();
        });
    }

}