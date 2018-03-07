/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.controllers;

import com.rdbcache.helpers.Cfg;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.Utils;

import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import com.rdbcache.models.Query;

import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.NotFoundException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import java.text.DecimalFormat;
import java.util.regex.Pattern;
import java.util.*;

@RestController
public class RdbcacheApis {

    private static final Logger LOGGER = LoggerFactory.getLogger(RdbcacheApis.class);

    private Pattern expPattern;

    private DecimalFormat durationFormat;

    @PostConstruct
    public void init() {
        expPattern = Pattern.compile("([0-9]+|-[0-9]+|\\+[0-9]+)");
        durationFormat = new DecimalFormat("#.######");
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

        Context context = new Context(key, true);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        if (key.equals("*")) {

            if (AppCtx.getDbaseRepo().findOne(context, keyInfo)) {

                AppCtx.getAsyncOps().doSaveToRedis(context, keyInfo);

            } else {

                throw new NotFoundException(context, "data not found");

            }
        } else {

            if (AppCtx.getRedisRepo().findOne(context, keyInfo)) {

                AppCtx.getAsyncOps().doSaveToDbase(context, keyInfo);

            } else if (AppCtx.getDbaseRepo().findOne(context, keyInfo)) {

                AppCtx.getAsyncOps().doSaveToRedis(context, keyInfo);

            } else {

                throw new NotFoundException(context, "data not found");

            }
        }
        return response(context);
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

        Context context = new Context(key, value, false);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        KvPair pair = context.getPair();
        AppCtx.getLocalCache().putData(pair.getId(), (Map<String, Object>) pair.getData(), keyInfo);

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, keyInfo);

        return response(context);
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

        Context context = new Context(key, value, false);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        KvPair pair = context.getPair();
        AppCtx.getLocalCache().putData(pair.getId(), (Map<String, Object>) pair.getData(), keyInfo);

        AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, keyInfo);

        return response(context);
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
        Context context = new Context(key, value, false);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        if (key.equals("*")) {

            AppCtx.getAsyncOps().doSaveToRedisAndDbase(context, keyInfo);

        } else {

            KvPair pair = context.getPair();
            AppCtx.getLocalCache().updateData(pair.getId(), (Map<String, Object>) pair.getData(), keyInfo);

            AppCtx.getAsyncOps().doPutOperation(context, keyInfo);

        }
        return response(context);
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
        Context context = new Context(key, value, true);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        Context ctx = context.getCopyWith(key, value);

        if (key.equals("*")) {

            AppCtx.getDbaseRepo().findOne(context, keyInfo);
            AppCtx.getAsyncOps().doSaveToRedis(ctx, keyInfo);

        } else if (AppCtx.getRedisRepo().findAndSave(context, keyInfo)) {

            AppCtx.getAsyncOps().doSaveToDbase(ctx, keyInfo);

        } else {

            AppCtx.getDbaseRepo().findOne(context, keyInfo);
            AppCtx.getAsyncOps().doSaveToDbase(ctx, keyInfo);

        }
        return response(context);
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

        Context context = new Context(key, value, true);
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, key, opt1, opt2);

        Context ctx = context.getCopyWith(key, value);

        if (key.equals("*")) {

            AppCtx.getDbaseRepo().findOne(context, keyInfo);
            AppCtx.getAsyncOps().doSaveToRedisAndDbase(ctx, keyInfo);

        } else if (AppCtx.getRedisRepo().findAndSave(context, keyInfo)) {

            AppCtx.getAsyncOps().doSaveToDbase(ctx, keyInfo);

        } else {

            AppCtx.getDbaseRepo().findOne(context, keyInfo);
            AppCtx.getAsyncOps().doSaveToDbase(ctx, keyInfo);

        }
        return response(context);
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
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, null, opt1, opt2);
        keyInfo.setIsNew(false);
        List<KvPair> pairs = context.getPairs();

        for (String key: keys) {
            KvPair pair = new KvPair(key);
            pairs.add(pair);
        }

        AppCtx.getRedisRepo().findAll(context, keyInfo);

        List<KvPair> redisPairs = new ArrayList<KvPair>();
        List<KvPair> dbPairs = new ArrayList<KvPair>();

        for (KvPair pair: pairs) {

            if (!pair.hasContent()) {

                Context ctx = context.getCopyWith(pair);
                KeyInfo dbKeyInfo = AppCtx.getKeyInfoRepo().findOne(ctx);

                if (dbKeyInfo != null && AppCtx.getDbaseRepo().findOne(ctx, dbKeyInfo)) {
                    dbPairs.add(pair);
                }

            } else {
                redisPairs.add(pair);
            }
        }

        if (redisPairs.size() > 0) {

            Context ctx = context.getCopyWith(redisPairs);
            AppCtx.getAsyncOps().doSaveToDbase(ctx, keyInfo);

        }

        if (dbPairs.size() > 0) {

            Context ctx = context.getCopyWith(dbPairs);
            AppCtx.getAsyncOps().doSaveToRedis(ctx, keyInfo);

        }

        return response(context, true);
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
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, null, opt1, opt2);
        keyInfo.setIsNew(false);
        List<KvPair> pairs = context.getPairs();

        for (Map.Entry<String, Object> entry: map.entrySet()) {

            KvPair pair = new KvPair(entry.getKey());
            Object value = entry.getValue();

            if (value instanceof Map) {

                pair.setData((Map<String, Object>) value);

            } else {

                Map<String, Object> pmap = Utils.toMap(value.toString());
                if (pmap == null && keyInfo.getTable() != null) {
                    throw new BadRequestException(context, "input for table is not a json");
                } else if (pmap == null) {

                    pmap = new LinkedHashMap<String, Object>();
                    pmap.put("_DEFAULT_", value);

                }
                pair.setData(pmap);

            }
            pairs.add(pair);
        }

        AppCtx.getAsyncOps().doPushOperations(context, keyInfo);

        return response(context, true);
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

        Context context = new Context(key, false);
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        AppCtx.getAsyncOps().doDeleteFromRedis(context);

        return response(context);
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
            @RequestBody ArrayList<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        List<KvPair> pairs = context.getPairs();

        for (String key: keys) {
            pairs.add(new KvPair(key));
        }

        AppCtx.getAsyncOps().doDeleteFromRedis(context);

        return response(context);
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

        Context context = new Context(key, false);
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context);

        return response(context);
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
            @RequestBody ArrayList<String> keys) {

        if (keys.contains("*")) {
            throw new BadRequestException("no * allowed as key");
        }

        Context context = new Context(false);
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        List<KvPair> pairs = context.getPairs();

        for (String key: keys) {
            pairs.add(new KvPair(key));
        }

        AppCtx.getAsyncOps().doDeleteFromRedisAndDbase(context);

        return response(context);
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
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, null, opt1, opt2);

        if (!AppCtx.getDbaseRepo().findAll(context, keyInfo)) {

            LOGGER.debug("findAll: no record found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, keyInfo);

        }

        return response(context, true);
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
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, null, opt1, opt2);
        List<KvPair> pairs = context.getPairs();

        for (String key: keys) {
            KvPair pair = null;
            if (key.equals("*")) {
                pair = new KvPair(Utils.generateId());
            } else {
                pair = new KvPair(key);
            }
            pairs.add(pair);
        }

        if (!AppCtx.getDbaseRepo().findAll(context, keyInfo)) {

            LOGGER.debug("findAll: no record found from database");

        } else {

            AppCtx.getAsyncOps().doSaveToRedis(context, keyInfo);

        }

        return response(context, true);
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
        KeyInfo keyInfo = setupContextAndKeyInfo(context, request, null, opt1, opt2);
        keyInfo.setIsNew(false);
        List<KvPair> pairs = context.getPairs();

        for (Map<String, Object> map: list) {

            KvPair pair = null;
            if (map.containsKey("key")) {

                String key = (String) map.get("key");
                map.remove("key");
                if (key.equals("*")) {
                    pair = new KvPair(Utils.generateId(), "data", map);
                } else {
                    pair = new KvPair(key, "data", map);
                }

            } else {

                pair = new KvPair(Utils.generateId(), "data", map);

            }
            pairs.add(pair);
        }

        AppCtx.getAsyncOps().doSaveAllToRedisAnInsertAllTodDbase(context, keyInfo);

        return response(context, true);
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
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(traceId, "trace"));
        if (dbPair != null) {
            context.setPair(dbPair);
        }

        return response(context);
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
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);
        List<KvPair> pairs = context.getPairs();

        for (String referenced_id: traceIds) {
            KvPair dbPair = AppCtx.getKvPairRepo().findOne(new KvIdType(referenced_id, "trace"));
            if (dbPair != null) {
                pairs.add(dbPair);
            } else {
                pairs.add(new KvPair(referenced_id));
            }
        }

        return response(context, true);
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

        Context context = new Context( true);
        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        if (!opt.isPresent()) {
            AppCtx.getLocalCache().removeAllKeyInfo();
            AppCtx.getLocalCache().removeAllData();
        } else {
            String action = opt.get();
            if (action.equals("all")) {
                AppCtx.getLocalCache().removeAll();
            } else if (action.equals("table")) {
                AppCtx.getLocalCache().removeAllTable();
            } else if (action.equals("key")) {
                AppCtx.getLocalCache().removeAllKeyInfo();
            } else if (action.equals("data")) {
                AppCtx.getLocalCache().removeAllData();
            }
        }

        return response(context);
    }

    private KeyInfo setupContextAndKeyInfo(
            Context context,
            HttpServletRequest request,
            String key,
            Optional<String> opt1, Optional<String> opt2) {

        if (Cfg.getEnableMonitor()) context.enableMonitor(request);

        KeyInfo keyInfo = null;
        if (key != null && !key.equals("*")) {
            keyInfo = AppCtx.getKeyInfoRepo().findOne(context);
        }
        if (keyInfo == null) {
            keyInfo = new KeyInfo();
        }

        String[] opts = {null, null}; // {expire, table}

        if (opt1!= null && opt1.isPresent()) {
            assignOption(context, opt1.get(), opts);
        }
        if (opt2 != null && opt2.isPresent()) {
            assignOption(context, opt2.get(), opts);
        }

        if (keyInfo.getIsNew()) {
            if (opts[1] != null) {
                keyInfo.setTable(opts[1]);
            }
            if (opts[0] != null) {
                keyInfo.setExpire(opts[0]);
            } else {
                keyInfo.setExpire(Cfg.getDefaultExpire());
            }
            Map<String, String[]> params = request.getParameterMap();
            if (params != null && params.size() > 0) {
                Query query = new Query(keyInfo.getTable());
                query.setConditionsFromParams(params);
                keyInfo.setQuery(query);
                keyInfo.setQueryKey(query.getKey());
            }
        } else {
            if (opts[0] != null && !opts[0].equals(keyInfo.getExpire())) {
                keyInfo.setExpire(opts[0]);
                keyInfo.setIsNew(true);
            }
            if (opts[1] != null && !opts[1].equals(keyInfo.getTable())) {
                throw new BadRequestException(context, "can not change table name for an existing key");
            }
            Map<String, String[]> params = request.getParameterMap();
            if (params != null && params.size() > 0) {
                Query query = new Query(keyInfo.getTable());
                query.setConditionsFromParams(params);
                if (keyInfo.getQueryKey() == null || !keyInfo.getQueryKey().equals(query.getKey())) {
                    throw new BadRequestException(context, "can not modify condition for an existing key");
                }
            }
        }

        LOGGER.debug("URI: "+ request.getRequestURI());
        LOGGER.trace("key: " + key + " " + (keyInfo == null ? "null" : keyInfo.toString()));

        return keyInfo;
    }

    private void assignOption(Context context, String opt, String[] opts) {

        opt = opt.trim();
        if (opts[0] == null && expPattern.matcher(opt).matches()) {
            opts[0] = opt;
            return;
        }
        if (opts[1] == null) {
            List<String> tables = AppCtx.getDbaseOps().getTableList(context);
            if (tables.contains(opt)) {
                opts[1] = opt;
                return;
            }
        }
        throw new BadRequestException(context, "invalid path variable " + opt);
    }

    private ResponseEntity<Map<String, Object>> response(
            Context context) {
        return response(context, false);
    }

    private ResponseEntity<Map<String, Object>> response(
            Context context,
            Boolean batch) {

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        Long now = System.currentTimeMillis();
        map.put("timestamp", now);
        Long duration = context.stopFirstStopWatch();
        if (duration != null) {
            double db = ((double) duration) / 1000000000.0;
            map.put("duration", durationFormat.format(db));
        }
        List<KvPair> pairs = context.getPairs();
        if (pairs != null) {
            if (pairs.size() == 0) {
                map.put("data", pairs);
            } else if (pairs.size() == 1 && !batch) {
                KvPair pair = pairs.get(0);
                map.put("key", pair.getId());
                if (context.ifSendValue()) {
                    map.put("data",pair.getMapValue());
                }
            } else {
                if (context.ifSendValue()) {
                    Map<String, Object> dmap = new LinkedHashMap<String, Object>();
                    map.put("data", dmap);
                    for (KvPair pair : pairs) {
                        dmap.put(pair.getId(), pair.getMapValue());
                    }
                } else {
                    List<String> keys = new ArrayList<>();
                    for (KvPair pair : pairs) {
                        keys.add(pair.getId());
                    }
                    map.put("data", keys);
                }
            }
        }
        String traceId = context.getTraceId();
        if ( traceId != null) {
            map.put("trace_id", traceId);
        }
        return ResponseEntity.ok(map);
    }
}