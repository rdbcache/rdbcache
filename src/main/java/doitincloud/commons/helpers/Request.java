/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.helpers;

import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.configs.PropCfg;
import doitincloud.commons.exceptions.BadRequestException;
import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.queries.QueryInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
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

        AnyKey anyKey = new AnyKey();

        if (pairs == null) {
            return anyKey;
        }

        // find key info for the first item in pairs
        //
        if (pairs.size() > 0) {
            AppCtx.getKeyInfoRepo().find(context, new KvPairs(pairs.getPair()), anyKey);
        }
        KeyInfo keyInfo = anyKey.getAny();

        processOptions(context, request, keyInfo, opts);

        if (pairs.size() == 0 || context.getAction().startsWith("select_")) {
            return anyKey;
        }

        // find key info for the second and after
        //
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

    private static void processOptions(Context context, HttpServletRequest request,
                                            KeyInfo keyInfo, Optional<String>[] opts) {

        String[] options = {null, null}; // {expire, table}

        for (int i = 0; i < opts.length; i++) {
            Optional<String> opt = opts[i];
            if (opt != null && opt.isPresent()) {
                assignOption(context, opt.get(), options);
            }
        }

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

    private static void assignOption(Context context, String opt, String[] opts) {

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
