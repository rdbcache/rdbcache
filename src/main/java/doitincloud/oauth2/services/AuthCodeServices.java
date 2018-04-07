package doitincloud.oauth2.services;

import doitincloud.commons.helpers.AnyKey;
import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.KvPairs;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.security.oauth2.common.exceptions.InvalidGrantException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.code.AuthorizationCodeServices;

import java.util.Map;

public class AuthCodeServices implements AuthorizationCodeServices {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthCodeServices.class);

    private static String type = "authorizationCode";

    private Context getContext() {
        Context context = new Context();
        return context;
    }

    @Override
    public String createAuthorizationCode(OAuth2Authentication authentication) {

        String code = Utils.generateId();

        KvIdType idType = new KvIdType(code, type);
        KvPair pair = new KvPair(idType);
        pair.setData(Utils.toMap(authentication));
        KeyInfo keyInfo = new KeyInfo();

        AppCtx.getCacheOps().putData(pair, keyInfo);

        Context context = getContext();

        AppCtx.getRedisRepo().save(context, pair, keyInfo);
        Utils.getExcutorService().submit(() -> {
            AppCtx.getDbaseRepo().save(context, pair, keyInfo);
        });

        return code;
    }

    @Override
    public OAuth2Authentication consumeAuthorizationCode(String code) throws InvalidGrantException {

        KvIdType idType = new KvIdType(code, type);
        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map != null) {
            LOGGER.trace("found code from cache " + code);
            return Utils.toPojo(map, OAuth2Authentication.class);
        }
        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo();

        Context context = getContext();

        if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found code from redis " + code);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            map = pair.getData();
            return Utils.toPojo(map, OAuth2Authentication.class);
        }
        if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found code from dbase " + code);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            Utils.getExcutorService().submit(() -> {
                AppCtx.getRedisRepo().save(context, pair, keyInfo);
            });
            map = pair.getData();
            return Utils.toPojo(map, OAuth2Authentication.class);
        }
        throw new InvalidGrantException("code " + code + " not found");
    }
}
