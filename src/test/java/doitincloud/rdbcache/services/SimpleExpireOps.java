package doitincloud.rdbcache.services;

import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.KvPairs;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleExpireOps extends ExpireOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleExpireOps.class);

    private String eventPrefix = "rdcevent";

    public void setExpireKey(Context context, KvPairs pairs, AnyKey anyKey) {
        for (int i = 0; i < pairs.size(); i++) {

            KvPair pair = pairs.get(i);
            String key = pair.getId();

            KeyInfo keyInfo = anyKey.getAny(i);

            LOGGER.debug("setExpireKey: " + key + " expire: " + keyInfo.getExpire());

            String expire = keyInfo.getExpire();
            String expKey = eventPrefix + "::" + key;

            boolean hasKey = AppCtx.getRedisRepo().ifExist(context, new KvPairs(expKey), new AnyKey(keyInfo));

            Long expValue = Long.valueOf(expire);

            boolean done = false;

            // remove existing expire key
            if (hasKey) {
                if (expValue <= 0L || expire.startsWith("+")) {

                    AppCtx.getRedisRepo().delete(context, new KvPairs(expKey), new AnyKey(keyInfo));

                } else {
                    // for unsigned expire, event existed, no update
                    done = true;
                }
            }

            // zero means no expiration
            if (!done && expValue == 0L) {
                done = true;
            }

            if (!done) {
                if (expValue < 0) {
                    expValue = -expValue;
                }

                LOGGER.debug("setup expire: " + key + " expire: " + keyInfo.getExpire());
                AppCtx.getStringRedisTemplate().opsForValue().set(expKey, expire, expValue);

            } else {
                keyInfo.restoreExpire();
            }
            if (keyInfo.getIsNew()) {
                LOGGER.debug("save keyInfo: " + key + " expire: " + keyInfo.getExpire());
                AppCtx.getKeyInfoRepo().save(context, new KvPairs(pair), new AnyKey(keyInfo));
            }
        }
   }
}