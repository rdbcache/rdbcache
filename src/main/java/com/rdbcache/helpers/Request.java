/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.exceptions.BadRequestException;
import com.rdbcache.models.KeyInfo;
import com.rdbcache.queries.Parser;
import com.rdbcache.queries.QueryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
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

    public static AnyKey process(Context context, HttpServletRequest request, String key,
                                 Optional<String> opt1, Optional<String> opt2) {

        if (PropCfg.getEnableMonitor()) context.enableMonitor(request);

        if (key == null && opt1 == null && opt2 == null) {
            return null;
        }

        AnyKey anyKey = new AnyKey();

        if (key != null && !key.equals("*")) {
            AppCtx.getKeyInfoRepo().find(context, new KvPairs(key), anyKey);
        }

        KeyInfo keyInfo = anyKey.getAny();

        if (key != null && key.equals("*")) {
            keyInfo.setGeneratedKey(true);
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
            }
            Map<String, String[]> params = request.getParameterMap();
            if (params != null && params.size() > 0) {
                QueryInfo queryInfo = new QueryInfo(keyInfo.getTable());
                Parser.setConditions(queryInfo, params);
                keyInfo.setQueryInfo(queryInfo);
                keyInfo.setQueryKey(queryInfo.getKey());
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
                QueryInfo queryInfo = new QueryInfo(keyInfo.getTable());
                Parser.setConditions(queryInfo, params);
                if (keyInfo.getQueryKey() == null || !keyInfo.getQueryKey().equals(queryInfo.getKey())) {
                    throw new BadRequestException(context, "can not modify condition for an existing key");
                }
            }
        }

        LOGGER.debug("URI: "+ request.getRequestURI());
        LOGGER.trace("key: " + key + " " + (keyInfo == null ? "" : keyInfo.toString()));

        return anyKey;
    }

    private static void assignOption(Context context, String opt, String[] opts) {

        opt = opt.trim();
        if (opts[0] == null && expPattern.matcher(opt).matches()) {
            opts[0] = opt;
            return;
        }
        if (opts[1] == null) {
            Map<String, Object> map = AppCtx.getDbaseOps().getTableList(context);
            List<String> tables = (List<String>) map.get("tables");
            if (tables.contains(opt)) {
                opts[1] = opt;
                return;
            }
        }
        throw new BadRequestException(context, "invalid path variable " + opt);
    }
}
