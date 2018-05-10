package doitincloud.rdbcache.repositories.impls;

import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import doitincloud.rdbcache.repositories.KvPairRepo;
import doitincloud.rdbcache.supports.DbUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
public class KvPairRepoImpl implements KvPairRepo {

    private String table = "rdbcache_kv_pair";

    private JdbcTemplate jdbcTemplate;

    @EventListener
    public void handleApplicationReadyEvent(ApplicationReadyEvent event) {
        jdbcTemplate = AppCtx.getSystemJdbcTemplate();
    }

    @Override
    public KvPair findById(KvIdType idType) {

        try {
            String sql = "select value from " + table + " where id = ? AND type = ?";
            Object[] params = new Object[]{idType.getId(), idType.getType()};
            String value = jdbcTemplate.queryForObject(sql, params, String.class);
            if (value == null) {
                return null;
            }
            KvPair pair = new KvPair(idType);
            pair.setValue(value);
            return pair;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public Iterable<KvPair> findAllById(List<KvIdType> idTypes) {

        List<KvPair> pairs = new ArrayList<>();
        for (KvIdType idType: idTypes) {
            pairs.add(findById(idType));
        }
        return pairs;
    }

    @Override
    public boolean save(KvPair pair) {
        KvPair dbPair = findById(pair.getIdType());
        if (dbPair == null) {
            try {
                String sql = "insert into " + table + " (id, type, value) values (?, ?, ?)";
                Object[] params = new Object[]{pair.getId(), pair.getType(), pair.getValue()};
                int result = jdbcTemplate.update(sql, params);
                return result == 1;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else if (!DbUtils.isMapEquals(pair.getData(), dbPair.getData())) {
            try {
                String sql = "update " + table + " set value = ? where id = ? AND type = ?";
                Object[] params = new Object[]{pair.getValue(), pair.getId(), pair.getType()};
                int result = jdbcTemplate.update(sql, params);
                return result == 1;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean saveAll(List<KvPair> pairs) {
        boolean allOk = true;
        for (KvPair pair: pairs) {
            if (!save(pair)) {
                allOk = false;
            }
        }
        return allOk;
    }

    @Override
    public boolean delete(KvPair pair) {
        try {
            String sql = "delete from " + table + " where  id = ? AND type = ?";
            Object[] params = new Object[]{pair.getId(), pair.getType()};
            int result = jdbcTemplate.update(sql, params);
            return result == 1;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean deleteAll(List<KvPair> pairs) {
        boolean allOk = true;
        for (KvPair pair: pairs) {
            if (!delete(pair)) {
                allOk = false;
            }
        }
        return allOk;
    }
}
