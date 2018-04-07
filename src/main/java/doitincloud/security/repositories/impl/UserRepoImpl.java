package doitincloud.security.repositories.impl;

import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.security.models.User;
import doitincloud.security.repositories.UserRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class UserRepoImpl implements UserRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepoImpl.class);

    private static String type = "userDetails";

    private static String table = "rdbcache_user_details";

    private static String indexKey = "username";

    private Context getContext() {
        Context context = new Context();
        return context;
    }

    @Override
    public User findByUsername(String username) {

        KvIdType idType = new KvIdType(username, type);
        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map != null) {
            LOGGER.trace("found user from cache: " + username);
            return Utils.toPojo(map, User.class);
        }

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, username);
        keyInfo.setIsNew(true);

        Context context = getContext();

        if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found user from redis: " + username);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            map = pair.getData();
            return Utils.toPojo(map, User.class);
        }
        if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found user from database: " + username);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            Utils.getExcutorService().submit(() -> {
                AppCtx.getRedisRepo().save(context, pair, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, pair, keyInfo);
            });
            map = pair.getData();
            return Utils.toPojo(map, User.class);
        }
        LOGGER.trace("user not found from anywhere: " + username);
        return null;
    }

    @Override
    public void save(User user) {

        String username = user.getUsername();
        Map<String, Object> map = Utils.toMap(user);

        KvPair pair = new KvPair(username, type, map);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, username);
        keyInfo.setIsNew(true);

        AppCtx.getCacheOps().putData(pair, keyInfo);

        Context context = getContext();

        AppCtx.getRedisRepo().save(context, pair, keyInfo);
        Utils.getExcutorService().submit(() -> {
            AppCtx.getExpireOps().setExpireKey(context, pair, keyInfo);
        });
        AppCtx.getDbaseRepo().save(context, pair, keyInfo);
    }

    @Override
    public void update(User user) {

        String username = user.getUsername();
        Map<String, Object> map = Utils.toMap(user);

        KvPair pair = new KvPair(username, type, map);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, username);
        keyInfo.setIsNew(true);

        AppCtx.getCacheOps().putData(pair, keyInfo);

        Context context = getContext();

        AppCtx.getCacheOps().putData(pair, keyInfo);
        AppCtx.getRedisRepo().save(context, pair, keyInfo);

        Utils.getExcutorService().submit(() -> {
            AppCtx.getExpireOps().setExpireKey(context, pair, keyInfo);
            AppCtx.getDbaseRepo().save(context, pair, keyInfo);
        });
    }

    @Override
    public void delete(String username) {

        KvIdType idType = new KvIdType(username, type);

        AppCtx.getCacheOps().removeData(idType);

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, username);

        Context context = getContext();

        AppCtx.getRedisRepo().delete(context, pair, keyInfo);
        AppCtx.getDbaseRepo().delete(context, pair, keyInfo);
        AppCtx.getKeyInfoRepo().delete(context, pair);

        AppCtx.getAsyncOps().deleteKvPairKeyInfo(context, pair, keyInfo);
    }
}
