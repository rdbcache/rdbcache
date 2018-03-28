/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.configs.PropCfg;
import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.exceptions.ServerErrorException;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.models.KvPair;
import com.rdbcache.queries.QueryInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class Request {

    private static final Logger LOGGER = LoggerFactory.getLogger(Request.class);

    private static Pattern expPattern = Pattern.compile("([0-9]+|-[0-9]+|\\+[0-9]+)");

    public static AnyKey process(Context context, HttpServletRequest request) {
        return process(context, request, null, null, null);
    }

    public static AnyKey process(Context context, HttpServletRequest request, KvPairs pairs) {
        return process(context, request, pairs, null, null);
    }

    public static AnyKey process(Context context, HttpServletRequest request, KvPairs pairs,
                                 Optional<String> opt1, Optional<String> opt2) {

        LOGGER.info("URI: "+ request.getRequestURI());

        if (PropCfg.getEnableMonitor()) context.enableMonitor(request);

        AnyKey anyKey = new AnyKey();

        if (pairs == null) {
            return anyKey;
        }

        // find the keyinfo for the first key
        if (pairs.size() > 0) {
            AppCtx.getKeyInfoRepo().find(context, new KvPairs(pairs.getPair()), anyKey);
        }
        KeyInfo keyInfo = anyKey.getAny();

        Map<String, String[]> params = request.getParameterMap();
        if ((params != null && params.size() > 0) ||opt1 != null || opt2 != null) {
            processOptions(context, keyInfo, params, opt1, opt2);
        }

        if (pairs.size() == 0 || context.getAction().startsWith("select_")) {
            return anyKey;
        }

        // find keyinfo for the second key and after
        if (pairs.size() > 1) {
            AppCtx.getKeyInfoRepo().find(context, pairs, anyKey);
        }

        for (int i = 0; i < pairs.size() && i < anyKey.size(); i++) {
            keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                keyInfo.setIsNew(false);
                String key = pairs.get(i).getId();
                AppCtx.getLocalCache().putKeyInfo(key, keyInfo);
                keyInfo.setIsNew(true);
            }
        }
        if (anyKey.size() != 1 && pairs.size() != anyKey.size()) {
            throw new ServerErrorException(context, "case not supported, anyKey size(" + anyKey.size() +
                    ") != 1 && pairs size(" + pairs.size() + ") != anyKey size(" + anyKey.size() + ")");
        }

        return anyKey;
    }

    private static QueryInfo processOptions(Context context, KeyInfo keyInfo, Map<String, String[]> params,
                                       Optional<String> opt1, Optional<String> opt2) {

        String[] opts = {null, null}; // {expire, table}

        if (opt1!= null && opt1.isPresent()) {
            assignOption(context, opt1.get(), opts);
        }

        if (opt2 != null && opt2.isPresent()) {
            assignOption(context, opt2.get(), opts);
        }

        QueryInfo queryInfo = null;

        if (keyInfo.getIsNew()) {
            if (opts[1] != null) {
                keyInfo.setTable(opts[1]);
            }
            if (opts[0] != null) {
                keyInfo.setExpire(opts[0]);
            }
            if (params != null && params.size() > 0) {
                queryInfo = new QueryInfo(keyInfo.getTable(), params);
                keyInfo.setQuery(queryInfo);
            }
        } else {
            if (opts[0] != null && !opts[0].equals(keyInfo.getExpire())) {
                keyInfo.setExpire(opts[0]);
                keyInfo.setIsNew(true);
            }
            if (opts[1] != null && !opts[1].equals(keyInfo.getTable())) {
                throw new BadRequestException(context, "can not change table name for an existing key");
            }
            if (params != null && params.size() > 0) {
                queryInfo = new QueryInfo(keyInfo.getTable(), params);
                if (keyInfo.getQueryKey() == null || !keyInfo.getQueryKey().equals(queryInfo.getKey())) {
                    throw new BadRequestException(context, "can not modify condition for an existing key");
                }
            }
        }
        return queryInfo;
    }

    private static void assignOption(Context context, String opt, String[] opts) {

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
        if (expPattern.matcher(opt).matches()) {
            throw new BadRequestException(context, "invalid path variable " + opt + ", expire already found");
        } else {
            throw new BadRequestException(context, "invalid path variable " + opt +
                    ", table not found OR missing primary/unique index");
        }
    }
}
