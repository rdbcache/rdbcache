package doitincloud.oauth2.repositories.impls;

import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.oauth2.models.Client;
import doitincloud.oauth2.repositories.ClientRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ClientRepoImpl implements ClientRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientRepoImpl.class);

    private static String type = "clientDetails";

    private static String table = "rdbcache_client_details";

    private static String indexKey = "clientId";

    private Context getContext() {
        Context context = new Context();
        return context;
    }

    @Override
    public Client findByClientId(String clientId) {

        KvIdType idType = new KvIdType(clientId, type);

        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map != null) {
            LOGGER.trace("found client from cache: " + clientId);
            return Utils.toPojo(map, Client.class);
        }

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, clientId);
        keyInfo.setIsNew(true);

        Context context = getContext();

        if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
            LOGGER.trace("found client from redis: " + clientId);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            map = pair.getData();
            return Utils.toPojo(map, Client.class);
        }
        if (AppCtx.getDbaseRepo().find(getContext(), pair, keyInfo)) {
            LOGGER.trace("found client from dbase: " + clientId);
            AppCtx.getCacheOps().putData(pair, keyInfo);
            Utils.getExcutorService().submit(() -> {
                AppCtx.getRedisRepo().save(context, pair, keyInfo);
                AppCtx.getExpireOps().setExpireKey(context, pair, keyInfo);
            });
            map = pair.getData();
            return Utils.toPojo(map, Client.class);
        }
        LOGGER.trace("client not found from anywhere: " + clientId);
        return null;
    }

    @Override
    public void save(Client client) {

        String clientId = client.getClientId();
        Map<String, Object> map = Utils.toMap(client);

        KvPair pair = new KvPair(clientId, type, map);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, clientId);
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
    public void update(Client client) {

        String clientId = client.getClientId();
        Map<String, Object> map = Utils.toMap(client);

        KvPair pair = new KvPair(clientId, type, map);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, clientId);
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
    public void delete(String clientId) {

        KvIdType idType = new KvIdType(clientId, type);

        AppCtx.getCacheOps().removeData(idType);

        KvPair pair = new KvPair(idType);
        KeyInfo keyInfo = new KeyInfo(table, indexKey, clientId);

        Context context = getContext();

        AppCtx.getRedisRepo().delete(context, pair, keyInfo);
        AppCtx.getDbaseRepo().delete(context, pair, keyInfo);
        AppCtx.getKeyInfoRepo().delete(context, pair);

        AppCtx.getAsyncOps().deleteKvPairKeyInfo(context, pair, keyInfo);
    }
}
