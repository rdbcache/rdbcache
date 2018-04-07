package doitincloud.oauth2.repositories.impls;

import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.Utils;
import doitincloud.oauth2.repositories.TokenRepo;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TokenRepoImpl<T> implements TokenRepo<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRepoImpl.class);

    private String type;
    private Class<T> tClass;

    public TokenRepoImpl(String type, Class<T> tClass) {
        this.type = type;
        this.tClass = tClass;
    }

    private Context getContext() {
        Context context = new Context();
        return context;
    }

    private Map<String, Object> getMap(String key) {

        KvIdType idType = new KvIdType(key, type);
        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map != null) {
            LOGGER.trace("found token fron cache " + key);
            return map;
        }

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo();

        Context context = getContext();

        if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found token fron redis " + key);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            map = pair.getData();
        } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found token fron dbase " + key);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            Utils.getExcutorService().submit(() -> {
                AppCtx.getRedisRepo().save(context, pair, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, pair, keyInfo);
            });
            map = pair.getData();
        }

        return map;
    }

    @Override
    public T get(String key) {
        Map<String, Object> map = getMap(key);
        if (map == null) {
            return null;
        }
        List<Object> list = Utils.convertMapToList(map);
        map = (Map<String, Object>) list.get(0);
        return Utils.toPojo(map, tClass);
    }

    @Override
    public Collection<T> getCollection(String key) {
        Map<String, Object> map = getMap(key);
        if (map == null) {
            return null;
        }
        List<Object> list = Utils.convertMapToList(map);
        List<T> tList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            map = (Map<String, Object>) list.get(0);
            tList.add(Utils.toPojo(map, tClass));
        }
        return tList;
    }

    @Override
    public String put(String key, T tObject) {

        Map<String, Object> map = Utils.toMap(tObject);
        Map<String, Object> listMap = new LinkedHashMap<>();
        listMap.put("0", map);

        KvPair pair = new KvPair(key, type, listMap);
        KeyInfo keyInfo = new KeyInfo();

        AppCtx.getCacheOps().putData(pair, keyInfo);

        Context context = getContext();

        AppCtx.getRedisRepo().save(context, pair, keyInfo);
        Utils.getExcutorService().submit(() -> {
            AppCtx.getDbaseRepo().save(context, pair, keyInfo);
        });
        return key;
    }

    @Override
    public void put(String key, Collection<T> tObjects) {

        Map<String, Object> listMap = new LinkedHashMap<>();
        int i = 0;
        for (T tObject: tObjects) {
            Map<String, Object> map = Utils.toMap(tObject);
            listMap.put(Integer.toString(i), map);
        }

        KvPair pair = new KvPair(key, type, listMap);
        KeyInfo keyInfo = new KeyInfo();

        Context context = getContext();

        AppCtx.getRedisRepo().save(context, pair, keyInfo);
        Utils.getExcutorService().submit(() -> {
            AppCtx.getDbaseRepo().save(context, pair, keyInfo);
        });
    }

    @Override
    public T remove(String key) {

        KvIdType idType = new KvIdType(key, type);

        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map != null) {
            AppCtx.getCacheOps().removeData(idType);
        }

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo();

        Context context = getContext();

        if (map == null) {
            if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
                map = pair.getData();
            } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
                map = pair.getData();
            }
        }
        if (map == null) {
            return null;
        }
        List<Object> list = Utils.convertMapToList(map);
        map = (Map<String, Object>) list.get(0);
        T tObject = Utils.toPojo(map, tClass);

        AppCtx.getDbaseRepo().delete(context, pair, keyInfo);
        AppCtx.getKvPairRepo().delete(pair);

        return tObject;
    }

    @Override
    public boolean containsKey(String key) {

        KvIdType idType = new KvIdType(key, type);
        if (AppCtx.getCacheOps().containsData(idType)) {
            return true;
        }

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo();

        Context context = getContext();

        if (AppCtx.getRedisRepo().ifExist(context, pair, keyInfo)) {
            return true;
        } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
            return true;
        }

        return false;
    }
}
