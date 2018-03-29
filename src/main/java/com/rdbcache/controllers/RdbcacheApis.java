/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.controllers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.*;

import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;

import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.NotFoundException;

import com.rdbcache.queries.QueryInfo;
import org.springframework.boot.context.event.ApplicationReadyEvent;
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

    @PostConstruct
    public void init() {
    }

    @EventListener
    public void handleEvent(ContextRefreshedEvent event) {
    }

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
    }

    /**
     * get_get get single item
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
            "/rdbcache/v1/get/{key}",
            "/rdbcache/v1/get/{key}/{opt1}",
            "/rdbcache/v1/get/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> get_get(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context(true);
        KvPairs pairs = new KvPairs(key);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        if (key.equals("*")) {
            if (AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

                AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

            } else {

                throw new NotFoundException(context, "data not found");
            }
        } else {
            if (!AppCtx.getRedisRepo().find(context, pairs, anyKey)) {

                if (AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

                    AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

                } else {

                    throw new NotFoundException(context, "data not found");
                }
            }
        }
        return Response.send(context, pairs);
    }

    /**
     * set_get get single item
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
            "/rdbcache/v1/set/{key}/{value}",
            "/rdbcache/v1/set/{key}/{value}/{opt1}",
            "/rdbcache/v1/set/{key}/{value}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> set_get(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable("value") String value,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context();
        KvPairs pairs = new KvPairs(key, value);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * set_post post single item
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
            "/rdbcache/v1/set/{key}",
            "/rdbcache/v1/set/{key}/{opt1}",
            "/rdbcache/v1/set/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> set_post(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(key, value);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * put_post post/put single item
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
            "/rdbcache/v1/put/{key}",
            "/rdbcache/v1/put/{key}/{opt1}",
            "/rdbcache/v1/put/{key}/{opt1}/{opt2}"
        }, method = {RequestMethod.POST, RequestMethod.PUT})
    public ResponseEntity<?> put_post(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(key, value);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        KeyInfo keyInfo = anyKey.getKeyInfo();

        if (key.equals("*") && keyInfo.getQuery() == null) {

            AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairs, anyKey);

        } else {

            AppCtx.getAsyncOps().doPutOperation(context, pairs, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * getset_get get single item
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
            "/rdbcache/v1/getset/{key}/{value}",
            "/rdbcache/v1/getset/{key}/{value}/{opt1}",
            "/rdbcache/v1/getset/{key}/{value}/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> getset_get(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable("value") String value,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context(true);
        KvPairs pairs = new KvPairs(key, value);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        KvPairs pairsClone = pairs.clone();

        if (key.equals("*")) {

            if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {
                pairs.getPair().clearData();
            }

            AppCtx.getAsyncOps().doSaveToRedis(context, pairsClone, anyKey);

        } else if (AppCtx.getRedisRepo().findAndSave(context, pairs, anyKey)) {

            AppCtx.getAsyncOps().doSaveToDbase(context, pairsClone, anyKey);

        } else {

            if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {
                if (pairs.size() > 0)  {
                    pairs.getPair().clearData();
                }
            }

            AppCtx.getAsyncOps().doSaveToDbase(context, pairsClone, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * getset_post post single item
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
            "/rdbcache/v1/getset/{key}",
            "/rdbcache/v1/getset/{key}/{opt1}",
            "/rdbcache/v1/getset/{key}/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> getset_post(
            HttpServletRequest request,
            @PathVariable("key") String key,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody String value) {

        if (value == null || value.length() == 0) {
            throw new BadRequestException("missing request body");
        }

        Context context = new Context(true);
        KvPairs pairs = new KvPairs(key, value);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        KvPairs pairsClone = pairs.clone();

        if (key.equals("*")) {

            if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {
                if (pairs.size() > 0) {
                    pairs.getPair().clearData();
                }
            }

            AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, pairsClone, anyKey);

        } else if (AppCtx.getRedisRepo().findAndSave(context, pairs, anyKey)) {

            AppCtx.getAsyncOps().doSaveToDbase(context, pairsClone, anyKey);

        } else {

            if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {
                pairs.getPair().clearData();
            }

            AppCtx.getAsyncOps().doSaveToDbase(context, pairsClone, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * pull_post post multiple items
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
            "/rdbcache/v1/pull",
            "/rdbcache/v1/pull/{opt1}",
            "/rdbcache/v1/pull/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> pull_post(
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

        Context context = new Context(true, true);
        KvPairs pairs = new KvPairs(keys);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        if (anyKey.size() != pairs.size()) {
            throw new NotFoundException("one or more keys not found");
        }

        for (int i = 0; i < anyKey.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                throw new NotFoundException("key not found for " + pair.getId());
            }
        }

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        if (!AppCtx.getRedisRepo().find(context, pairs, anyKey)) {

            KvPairs dbPairs = new KvPairs();

            for (int i = 0; i < pairs.size(); i++) {

                KvPair pair = pairs.get(i);
                if (!pair.hasContent()) {

                    KeyInfo keyInfo = anyKey.get(i);
                    KvPairs pairsNew = new KvPairs(pair);
                    AnyKey anyKeyNew = new AnyKey(keyInfo);

                    if (AppCtx.getDbaseRepo().find(context, pairsNew, anyKeyNew)) {
                        dbPairs.add(pair);
                    }
                }
            }

            if (dbPairs.size() > 0) {
                AppCtx.getAsyncOps().doSaveToRedis(context, dbPairs, anyKey);
            }
        }

        return Response.send(context, pairs);
    }

    /**
     * push_post post multiple items
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
            "/rdbcache/v1/push",
            "/rdbcache/v1/push/{opt1}",
            "/rdbcache/v1/push/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> push_post(
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

        Context context = new Context(false, true);
        KvPairs pairs = new KvPairs(map);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        if (anyKey.size() != map.size()) {
            throw new BadRequestException("one or more keys not found");
        }

        for (int i = 0; i < anyKey.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                throw new BadRequestException("key not found for " + pair.getId());
            }
        }

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doPushOperations(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * delkey_get get/delete single item
     *
     * To delete a key from redis based on the input key. No query string.
     * It returns immediately. It will not delete database entry.
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/delkey/{key}"
    }, method = {RequestMethod.GET, RequestMethod.DELETE})
    public ResponseEntity<?> delkey_get(
            HttpServletRequest request,
            @PathVariable("key") String key) {

        if (key.equals("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(key);
        AnyKey anyKey = Request.process(context, request, pairs);

        if (anyKey.getKeyInfo().getIsNew()) {
            throw new NotFoundException("key not found for " + key);
        }

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doDeleteFromRedis(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * delkey_post post multiple items
     *
     * To delete one or more keys from redis based on the input keys. No query string.
     * It returns immediately. It will not delete database entry.
     *
     * @param request HttpServletRequest
     * @param keys List, list of keys for returned entries
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/delkey"
    }, method = RequestMethod.POST)
    public ResponseEntity<?> delkey_post(
            HttpServletRequest request,
            @RequestBody List<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(keys);
        AnyKey anyKey = Request.process(context, request, pairs);

        if (anyKey.size() != keys.size()) {
            context.logTraceMessage("one or more keys not found");
        }

        for (int i = 0; i < anyKey.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                context.logTraceMessage("key not found for " + pair.getId());
            }
        }

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doDeleteFromRedis(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * delall_get get single item
     *
     * To delete a key from redis and database based on the input key. No query string.
     * It returns immediately.
     *
     * @param request HttpServletRequest
     * @param key String, hash key
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/delall/{key}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> delall_get(
            HttpServletRequest request,
            @PathVariable("key") String key) {

        if (key.equals("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(key);
        AnyKey anyKey = Request.process(context, request, pairs);

        if (anyKey.getKeyInfo().getIsNew()) {
            throw new NotFoundException("key not found for " + key);
        }

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * delall_post post multple items
     *
     * To delete one or more keys from redis and database based on the input keys. No query string.
     * It returns immediately.
     *
     * @param request HttpServletRequest
     * @param keys List, list of keys for returned entries
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/delall"
    }, method = RequestMethod.POST)
    public ResponseEntity<?> delall_post(
            HttpServletRequest request,
            @RequestBody List<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context();
        KvPairs pairs = new KvPairs(keys);
        AnyKey anyKey = Request.process(context, request, pairs);

        if (anyKey.size() != keys.size()) {
            context.logTraceMessage("one or more keys not found");
        }

        for (int i = 0; i < anyKey.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                context.logTraceMessage("key not found for " + pair.getId());
            }
        }

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * select_get get multiple items
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
            "/rdbcache/v1/select",
            "/rdbcache/v1/select/{opt1}",
            "/rdbcache/v1/select/{opt1}/{opt2}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> select_get(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2) {

        Context context = new Context(true, true);
        KvPairs pairs = new KvPairs();
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        KeyInfo keyInfo = anyKey.getKeyInfo();
        if (keyInfo.getQuery() == null && pairs.size() == 0) {
            QueryInfo query = new QueryInfo(keyInfo.getTable());
            query.setLimit(1024);
            keyInfo.setQuery(query);
            String msg = "no query string found, max rows limit is forced to 1024";
            LOGGER.info(msg);
            context.logTraceMessage(msg);
        }

        if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

            LOGGER.debug("no record(s) found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);

        }

        return Response.send(context, pairs);
    }

    /**
     * select_post post multiple items
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
            "/rdbcache/v1/select",
            "/rdbcache/v1/select/{opt1}",
            "/rdbcache/v1/select/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> select_post(
            HttpServletRequest request,
            @PathVariable Optional<String> opt1,
            @PathVariable Optional<String> opt2,
            @RequestBody ArrayList<String> keys) {

        Context context = new Context(true, true);
        KvPairs pairs = new KvPairs(keys);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        KeyInfo keyInfo = anyKey.getKeyInfo();
        if (keyInfo.getQuery() == null && pairs.size() == 0) {
            QueryInfo query = new QueryInfo(keyInfo.getTable());
            query.setLimit(1024);
            keyInfo.setQuery(query);
            String msg = "no query string found, max rows limit is forced to 1024";
            LOGGER.info(msg);
            context.logTraceMessage(msg);
        }

        if (!AppCtx.getDbaseRepo().find(context, pairs, anyKey)) {

            LOGGER.debug("no record(s) found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, pairs, anyKey);
        }

        return Response.send(context, pairs);
    }

    /**
     * save_post post multiple items
     *
     * To save one or more entries based on input list.
     * It returns immediately, and asynchronously inserts into redis and database
     *
     * @param request HttpServletRequest
     * @param opt1 String, can be expire or table
     * @param opt2 String, can be expire or table, but not otp1
     * @param list List, a list of map, than contains key and other fields
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/save",
            "/rdbcache/v1/save/{opt1}",
            "/rdbcache/v1/save/{opt1}/{opt2}"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> save_post(
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

        Context context = new Context(false, true);
        KvPairs pairs = new KvPairs(list);
        AnyKey anyKey = Request.process(context, request, pairs, opt1, opt2);

        LOGGER.trace(anyKey.print() + " pairs(" + pairs.size() +"): " + pairs.printKey());

        AppCtx.getAsyncOps().doSaveAllToRedisAndSaveAllTodDbase(context, pairs, anyKey);

        return Response.send(context, pairs);
    }

    /**
     * trace_get get single item
     *
     * get error messages by trace id
     *
     * @param request HttpServletRequest
     * @param traceId the trace id return by API call
     * @return ResponseEntity
     *
     */
    @RequestMapping(value = {
            "/rdbcache/v1/trace/{traceId}"
        }, method = RequestMethod.GET)
    public ResponseEntity<?> trace_get(
            HttpServletRequest request,
            @PathVariable("traceId") String traceId){

        if (traceId.equals("*")) {
            throw new BadRequestException("no * allowed as trace id");
        }
        if (request.getParameterMap().size() != 0) {
            throw  new BadRequestException("no query string is allowed");
        }

        Context context = new Context(true);
        KvPairs pairs = new KvPairs();
        Request.process(context, request, pairs);

        LOGGER.trace("pairs(" + pairs.size() +"): " + pairs.printKey());

        KvPair pairKey = new KvPair(traceId, "trace");
        Optional<KvPair> opt = AppCtx.getKvPairRepo().findById(new KvIdType(traceId, "trace"));
        if (opt.isPresent()) {
            pairs.add(opt.get());
        }

        return Response.send(context, pairs);
    }

    /**
     * trace_post post multiple items
     *
     * get error messages by trace id list
     *
     * @param request HttpServletRequest
     * @param traceIds List trace id list
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/trace"
        }, method = RequestMethod.POST)
    public ResponseEntity<?> trace_post(
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

        Context context = new Context( true, true);
        KvPairs pairs = new KvPairs();
        Request.process(context, request, pairs);

        LOGGER.trace("pairs(" + pairs.size() +"): " + pairs.printKey());

        for (String referenced_id: traceIds) {
            Optional<KvPair> optional = AppCtx.getKvPairRepo().findById(new KvIdType(referenced_id, "trace"));
            if (optional.isPresent()) {
                pairs.add(optional.get());
            } else {
                pairs.add(new KvPair(referenced_id));
            }
        }

        return Response.send(context, pairs);
    }

    /**
     * flushcache_get get operational to multiple items
     *
     * flush local cache
     *
     * @param request HttpServletRequest
     * @param opt optional, all, table, key and data
     * @return ResponseEntity
     */
    @RequestMapping(value = {
            "/rdbcache/v1/flush-cache",
            "/rdbcache/v1/flush-cache/{opt}"
    }, method = RequestMethod.GET)
    public ResponseEntity<?> flushcache_get(
            HttpServletRequest request,
            @PathVariable Optional<String> opt) {

        Context context = new Context();
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