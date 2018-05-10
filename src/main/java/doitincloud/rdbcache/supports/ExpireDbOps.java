package doitincloud.rdbcache.supports;

public interface ExpireDbOps {

    public boolean save(final Context context, final KvPairs pairs, final AnyKey anyKey);

}
