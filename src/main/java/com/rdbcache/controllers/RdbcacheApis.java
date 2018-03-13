/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.controllers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.helpers.*;

import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;

import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.NotFoundException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import java.util.*;

@RestController
public class RdbcacheApis {

    private static final Logger LOGGER = LoggerFactory.getLogger(RdbcacheApis.class);

    private boolean enableLocalCache = true;

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
        if (PropCfg.getKeyMinCacheTTL() <= 0l && PropCfg.getDataMaxCacheTLL() <= 0l) {
            enableLocalCache = false;
        }
    }

    public boolean isEnableLocalCache() {
        return enableLocalCache;
    }

    public void setEnableLocalCache(boolean enableLocalCache) {
        this.enableLocalCache = enableLocalCache;
    }

    /**
     * get
     *
     * To get data based on key and/or query string.
     * Once data found, it returns immediately. It queries redis first, then database. 
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/get/{key}",
            "/v1/get/{key}/{opt1}",
            "/v1/get/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> get(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key);

        if (key.equals("*")) {
            if (AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

                AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

            } else {

                throw new NotFoundException(context, "data not found");

            }
        } else {
            if (AppCtx.getRedisRepo().find(context, pairs, anyKey)) {

                AppCtx.getAsyncOps().doSaveToDbase(context, pairs, anyKey);

            } else if (AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

                AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

            } else {
                throw new NotFoundException(context, "data not found");
            }
        }
        return Response.send(context, pairs);
    }

    /**
     * set
     *
     * To set a value to a key based on the key and/or query string.
     * It returns immediately, and asynchronously saves to redis and database
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param value String, value
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/set/{key}/{value}",
            "/v1/set/{key}/{value}/{opt1}",
            "/v1/set/{key}/{value}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> set(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable("value") String value,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context(false);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key, value);

        if (enableLocalCache) {

            AppCtx.getLocalCache().putContextData(pairs, anyKey);
        }

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * setpost
     *
     * To set a value to a key based on the key and/or query string.
     * It returns immediately, and asynchronously saves to redis and database
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/set/{key}",
            "/v1/set/{key}/{opt1}",
            "/v1/set/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> setpost(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context(false);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key, value);

        if (enableLocalCache) {

            AppCtx.getLocalCache().putContextData(pairs, anyKey);
        }

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * put
     *
     * To update a key with partial data based on the key and/or query string.
     * It returns immediately, and asynchronously updates to redis and database
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/put/{key}",
            "/v1/put/{key}/{opt1}",
            "/v1/put/{key}/{opt1}/{opt2}"
        }, method = {RequestMethod.POST, RequestMethod.PUT})
    public ResponseEntity<?> put(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }
        Context context = new Context(false);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key, value);

        if (key.equals("*")) {

            AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        } else {

            if (enableLocalCache) {

                AppCtx.getLocalCache().updatePairsData(pairs, anyKey);
            }

            AppCtx.getAsyncOps().doPutOperation(context, pairs, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * getset
     *
     * To get current value of a key and update it to a new value based on the key and/or query string.
     * It finds the current value and returns immediately, and asynchronously updates to redis and database
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param value String, value
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/getset/{key}/{value}",
            "/v1/getset/{key}/{value}/{opt1}",
            "/v1/getset/{key}/{value}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> getset(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable("value") String value,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing value");
        }
        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key, value);

        KvPairs pairs2 = new KvPairs(key, value);

        if (key.equals("*")) {

            AppCtx.getDbaseRepo().find(context, pairs, anyKey);

            AppCtx.getAsyncOps().doSaveToRedis(context, pairs2, anyKey);

        } else if (AppCtx.getRedisRepo().findAndSave(context, pairs, anyKey)) {

            AppCtx.getAsyncOps().doSaveToDbase(context, pairs2, anyKey);

        } else {

            AppCtx.getDbaseRepo().find(context, pairs, anyKey);

            AppCtx.getAsyncOps().doSaveToDbase(context, pairs2, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * getset
     *
     * To get current value of a key and update it to a new value based on key and/or query string.
     * It finds the current value and returns immediately, and asynchronously updates to redis and database
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/getset/{key}",
            "/v1/getset/{key}/{opt1}",
            "/v1/getset/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> getsetpost(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, key, opt1, opt2);

        KvPairs pairs = new KvPairs(key, value);
        KvPairs pairs2 = new KvPairs(key, value);

        if (key.equals("*")) {

            AppCtx.getDbaseRepo().find(context, pairs, anyKey);

            AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs2, anyKey);

        } else if (AppCtx.getRedisRepo().findAndSave(context, pairs, anyKey)) {

            AppCtx.getAsyncOps().doSaveToDbase(context, pairs2, anyKey);

        } else {

            AppCtx.getDbaseRepo().find(context, pairs, anyKey);

            AppCtx.getAsyncOps().doSaveToDbase(context, pairs2, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * pull
     *
     * To pull one or more entries based on input keys. No * key. No query string.
     * Once data found, it returns immediately. It queries redis first, then database. 
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/pull",
            "/v1/pull/{opt1}",
            "/v1/pull/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> pull(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody ArrayList<String> keys) {

        if (keys == null || keys.size() == 0) {
            throw new BadRequestException("missing keys");
        }
        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }
        if (request.getParameterMap().size() > 0) {
            throw  new BadRequestException("query string is not supported");
        }

        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, null, opt1, opt2);

        KvPairs pairs = new KvPairs(keys);

        AppCtx.getRedisRepo().find(context, pairs, anyKey);

        KvPairs redisPairs = new KvPairs();
        KvPairs dbPairs = new KvPairs();

        for (KvPair pair: pairs) {

            if (!pair.hasContent()) {

                AnyKey anyKey2 = new AnyKey(new KeyInfo());
                KvPairs pairs2 = new KvPairs(pair);

                if (AppCtx.getKeyInfoRepo().find(context, pairs2, anyKey2) &&
                    AppCtx.getDbaseRepo().find(context, pairs2, anyKey2)) {

                    dbPairs.add(pair);
                }
            } else {

                redisPairs.add(pair);
            }
        }

        if (redisPairs.size() > 0) {
            AppCtx.getAsyncOps().doSaveToDbase(context, redisPairs, anyKey);
        }

        if (dbPairs.size() > 0) {
            AppCtx.getAsyncOps().doSaveToRedis(context, dbPairs, anyKey);
        }

        return Response.send(context, pairs, true);
    }

    /**
     * push
     *
     * To update one or more entries based on input key and value map. No * key. No query string.
     * It returns immediately, and asynchronously updates redis and database
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @param map Map, a map of key and value pairs
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/push",
            "/v1/push/{opt1}",
            "/v1/push/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> push(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody Map<String, Object> map) {

        if (map == null || map.size() == 0) {
            throw new BadRequestException("missing request body");
        }
        if (map.containsKey("*")) {
            throw new BadRequestException("no * allowed as key");
        }
        if (request.getParameterMap().size() > 0) {
            throw new BadRequestException("query string is not supported");
        }

        Context context = new Context(false);
        AnyKey anyKey = Request.process(context, request, null, opt1, opt2);

        KvPairs pairs = new KvPairs(map);

        AppCtx.getAsyncOps().doPushOperations(context, pairs, anyKey);

        return Response.send(context, pairs, true);
    }

    /**
     * delkey
     *
     * To delete a key from redis based on the input key. No query string.
     * It returns immediately. It will not delete database entry.
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/delkey/{key}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> delkey(
            HttpServletRequest request,
            @PathVariable("key") String key) {

        if (key.equals("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        Request.process(context, request);

        KvPairs pairs = new KvPairs(key);

        AppCtx.getAsyncOps().doDeleteFromRedis(context, pairs);

        return Response.send(context, pairs);
    }

    /**
     * delkeyepost
     *
     * To delete one or more keys from redis based on the input keys. No query string.
     * It returns immediately. It will not delete database entry.
     *
     * @param request HttpServletRequest
     * @param keys List, list of keys for returned entries
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/delkey"
    }, method = RequestMethod.POST)
    public ResponseEntity<?> delkeypost(
            HttpServletRequest request,
            @RequestBody List<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        Request.process(context, request);

        KvPairs pairs = new KvPairs(keys);

        AppCtx.getAsyncOps().doDeleteFromRedis(context, pairs);

        return Response.send(context, pairs);
    }

    /**
     * delall
     *
     * To delete a key from redis and database based on the input key. No query string.
     * It returns immediately.
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/delall/{key}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> delall(
            HttpServletRequest request,
            @PathVariable("key") String key) {

        if (key.equals("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        Request.process(context, request);

        KvPairs pairs = new KvPairs(key);

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context, pairs);

        return Response.send(context, pairs);
    }

    /**
     * delallpost
     *
     * To delete one or more keys from redis and database based on the input keys. No query string.
     * It returns immediately.
     *
     * @param request HttpServletRequest
     * @param keys List, list of keys for returned entries
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/delall"
    }, method = RequestMethod.POST)
    public ResponseEntity<?> delallpost(
            HttpServletRequest request,
            @RequestBody List<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        Request.process(context, request);

        KvPairs pairs = new KvPairs(keys);

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context, pairs);

        return Response.send(context, pairs);
    }

    /**
     * select
     *
     * To select one or more entries based on query string.
     * It queries database and return immediately, and asynchronously saves the data to redis
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/select",
            "/v1/select/{opt1}",
            "/v1/select/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> select(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        if (request.getParameterMap().size() == 0) {
            throw  new BadRequestException("query string is missing");
        }

        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, null, opt1, opt2);

        KvPairs pairs = new KvPairs();

        if (!AppCtx.getDbaseRepo().find(context, pairs,  anyKey)) {

            LOGGER.debug("find: no record found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

        }

        return Response.send(context, pairs, true);
    }

    /**
     * select
     *
     * To select one or more entries based on query string.
     * It queries database and return immediately, and asynchronously saves the data to redis
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @param keys List, list of keys for returned entries
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/select",
            "/v1/select/{opt1}",
            "/v1/select/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> querypost(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody ArrayList<String> keys) {

        if (request.getParameterMap().size() == 0) {
            throw  new BadRequestException("query string is missing");
        }

        Context context = new Context(true);
        AnyKey anyKey = Request.process(context, request, null, opt1, opt2);

        KvPairs pairs = new KvPairs(keys);

        if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

            LOGGER.debug("findAll: no record found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);
        }

        return Response.send(context, pairs, true);
    }

    /**
     * insert
     *
     * To insert one or more entries based on input list.
     * It returns immediately, and asynchronously inserts into redis and database
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @param list List, a list of map, than contains key and other fields
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/insert",
            "/v1/insert/{opt1}",
            "/v1/insert/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> insert(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody List<Map<String, Object>> list){

        if (request.getParameterMap().size() != 0) {
            throw  new BadRequestException("no query string is needed");
        }
        if (list == null || list.size() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context(false);
        AnyKey anyKey = Request.process(context, request, null, opt1, opt2);

        KvPairs pairs = new KvPairs(list);

        AppCtx.getAsyncOps().doSaveAllToRedisAnInsertAllTodDbase(context, pairs, anyKey);

        return Response.send(context, pairs, true);
    }

    /**
     * trace
     *
     * get error messages by trace id
     *
     * @param request HttpServletRequest
     * @param traceId the trace id return by API call
     * @return ResponseEntity
     *
     */
    @RequestMapping(value = {
            "/v1/trace/{traceId}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> trace(
            HttpServletRequest request,
            @PathVariable("traceId") String traceId){

        if (request.getParameterMap().size() != 0) {
            throw  new BadRequestException("no query string is needed");
        }

        Context context = new Context(true);
        Request.process(context, request);

        KvPairs pairs = new KvPairs();

        KvPair pair = AppCtx.getKvPairRepo().findOne(new KvIdType(traceId, "trace"));
        if (pair != null) {
            pairs.add(pair);
        }

        return Response.send(context, pairs);
    }

    /**
     * tracepost
     *
     * get error messages by trace id list
     *
     * @param request HttpServletRequest
     * @param traceIds List trace id list
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/trace"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> tracepost(
            HttpServletRequest request,
            @RequestBody List<String> traceIds){

        if (traceIds == null || traceIds.size() == 0) {
            throw new BadRequestException("missing trace ids");
        }
        if (traceIds.contains("*")) {
            throw new BadRequestException("no * allowed as trace id");
        }
        if (request.getParameterMap().size() != 0) {
            throw  new BadRequestException("no query string is needed");
        }

        Context context = new Context( true);
        Request.process(context, request);

        KvPairs pairs = new KvPairs();

        for (String referenced_id: traceIds) {
            KvPair pair = AppCtx.getKvPairRepo().findOne(new KvIdType(referenced_id, "trace"));
            if (pair != null) {
                pairs.add(pair);
            } else {
                pairs.add(new KvPair(referenced_id));
            }
        }

        return Response.send(context, pairs, true);
    }

    /**
     * flushCache
     *
     * flush local cache
     *
     * @param request HttpServletRequest
     * @param opt optional, all, table, key and data
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/v1/flush-cache",
            "/v1/flush-cache/{opt}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> flushCache(
            HttpServletRequest request,
            @PathVariable Optional<String> opt) {

        Context context = new Context(false);
        Request.process(context, request);

        if (!opt.isPresent()) {
            AppCtx.getLocalCache().removeAllKeyInfos();
            AppCtx.getLocalCache().removeAllData();
        } else {
            String action = opt.get();
            if (action.equals("all")) {
                AppCtx.getLocalCache().removeAll();
            } else if (action.equals("table")) {
                AppCtx.getLocalCache().removeAllTables();
            } else if (action.equals("key")) {
                AppCtx.getLocalCache().removeAllKeyInfos();
            } else if (action.equals("data")) {
                AppCtx.getLocalCache().removeAllData();
            }
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("result", "DONE");

        return Response.send(context, data);
    }
}