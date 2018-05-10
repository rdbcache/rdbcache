/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.controllers.supports;

import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.KvPairs;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.exceptions.BadRequestException;
import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.queries.QueryInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class Request {

    private static final Logger LOGGER = LoggerFactory.getLogger(Request.class);

    private static Pattern expPattern = Pattern.compile("([0-9]+|-[0-9]+|\\+[0-9]+)(-sync)?$");

    public static AnyKey process(Context context, HttpServletRequest request) {
        return process(context, request, null);
    }

    public static AnyKey process(Context context, HttpServletRequest request, KvPairs pairs,
                                 Optional<String> ... opts) {

        LOGGER.info("URI: "+ request.getRequestURI());

        if (PropCfg.getEnableMonitor()) context.enableMonitor(request);

        String[] options = {null, null}; // {expire, table}

        for (int i = 0; i < opts.length; i++) {
            Optional<String> opt = opts[i];
            if (opt != null && opt.isPresent()) {
                assignOption(context, opt.get(), options);
            }
        }

        AnyKey anyKey = new AnyKey();

        if (pairs == null) {
            return anyKey;
        }

        KeyInfo keyInfo = anyKey.getAny();
        String table = options[1];

        if (pairs.size() > 0) {

            if (table != null) {
                // populate table info into all pairs
                for (KvPair pair: pairs) {
                    pair.setType(table);
                }
            }

            // find key info for the first pair
            //
            KvPair pair = pairs.get(0);
            if (!pair.isNewUuid()) {
                AppCtx.getKeyInfoRepo().find(context, pair, keyInfo);
            }
        }

        processOptions(context, request, keyInfo, options);

        if (pairs.size() == 0) {
            return anyKey;
        }

        // query string precedes all caches
        //
        if (keyInfo.getQuery() != null) {
            return anyKey;
        }

        // find key info for the second and after
        //
        if (pairs.size() > 1) {
            AppCtx.getKeyInfoRepo().find(context, pairs, anyKey);
        }

        // save key info to local cahce
        //
        for (int i = 0; i < pairs.size() && i < anyKey.size(); i++) {
            keyInfo = anyKey.get(i);
            if (keyInfo.getIsNew()) {
                keyInfo.setIsNew(false);
                KvPair pair = pairs.get(i);
                AppCtx.getCacheOps().putKeyInfo(pair.getIdType(), keyInfo);
                keyInfo.setIsNew(true);
            }
        }

        if (anyKey.size() != 1 && pairs.size() != anyKey.size()) {
            throw new ServerErrorException(context, "case not supported, anyKey size(" + anyKey.size() +
                    ") != 1 && pairs size(" + pairs.size() + ") != anyKey size(" + anyKey.size() + ")");
        }

        return anyKey;
    }

    private static void processOptions(Context context, HttpServletRequest request,
                                            KeyInfo keyInfo, String[] options) {

        Map<String, String[]> params = request.getParameterMap();

        if (keyInfo.getIsNew()) {
            if (options[1] != null) {
                keyInfo.setTable(options[1]);
            }
            if (options[0] != null) {
                keyInfo.setExpire(options[0]);
            }
            if (params != null && params.size() > 0) {
                QueryInfo queryInfo = new QueryInfo(keyInfo.getTable(), params);
                keyInfo.setQuery(queryInfo);
            }
        } else {
            if (options[0] != null && !options[0].equals(keyInfo.getExpire())) {
                keyInfo.setExpire(options[0]);
                keyInfo.setIsNew(true);
            }
            if (options[1] != null && !options[1].equals(keyInfo.getTable())) {
                throw new BadRequestException(context, "can not change table name for an existing key");
            }
            if (params != null && params.size() > 0) {
                QueryInfo queryInfo = new QueryInfo(keyInfo.getTable(), params);
                if (keyInfo.getQueryKey() == null || !keyInfo.getQueryKey().equals(queryInfo.getKey())) {
                    throw new BadRequestException(context, "can not modify condition for an existing key");
                }
            }
        }
    }

    private static void assignOption(Context context, String opt, String[] options) {

        opt = opt.trim();
        if (opt.equals("async")) {
            if (context.isSync()) {
                context.setSync(false);
            } else {
                LOGGER.trace("default is async, no need to have option async");
            }
            return;
        }
        if (opt.equals("sync")) {
            if (context.isSync()) {
                LOGGER.trace("default is sync, no need to have option sync");
            } else {
                context.setSync(true);
            }
            return;
        }
        if (opt.equals("delayed")) {
            if (context.isSync()) {
                LOGGER.trace("default is delayed, no need to have option delayed");
            } else {
                context.setDelayed();
            }
            return;
        }
        if (options[0] == null && expPattern.matcher(opt).matches()) {
            options[0] = opt;
            return;
        }
        if (options[1] == null) {
            Map<String, Object> tables = AppCtx.getDbaseOps().getTablesMap(context);
            if (tables.containsKey(opt)) {
                options[1] = opt;
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
