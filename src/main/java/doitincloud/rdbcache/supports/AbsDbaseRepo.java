package doitincloud.rdbcache.supports;

import doitincloud.rdbcache.supports.AnyKey;
import doitincloud.rdbcache.supports.Context;
import doitincloud.rdbcache.supports.KvPairs;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.repositories.DbaseRepo;

public abstract class AbsDbaseRepo implements DbaseRepo {

    @Override
    public boolean find(final Context context, final KvPairs pairs, final AnyKey anyKey) {
        boolean allOk = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            if (!find(context, pair, keyInfo)) {
                allOk = false;
            }
        }
        return allOk;
    }

    @Override
    public boolean save(final Context context, final KvPairs pairs, final AnyKey anyKey) {
        boolean allOk = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            if (!save(context, pair, keyInfo)) {
                allOk = false;
            }
        }
        return allOk;
    }

    @Override
    public boolean insert(final Context context, final KvPairs pairs, final AnyKey anyKey) {
        boolean allOk = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            if (!insert(context, pair, keyInfo)) {
                allOk = false;
            }
        }
        return allOk;
    }

    @Override
    public boolean update(final Context context, final KvPairs pairs, final AnyKey anyKey) {
        boolean allOk = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            if (!update(context, pair, keyInfo)) {
                allOk = false;
            }
        }
        return allOk;
    }

    @Override
    public boolean delete(final Context context, final KvPairs pairs, final AnyKey anyKey) {
        boolean allOk = true;
        for (int i = 0; i < pairs.size(); i++) {
            KvPair pair = pairs.get(i);
            KeyInfo keyInfo = anyKey.getAny(i);
            if (!delete(context, pair, keyInfo)) {
                allOk = false;
            }
        }
        return allOk;
    }
}
