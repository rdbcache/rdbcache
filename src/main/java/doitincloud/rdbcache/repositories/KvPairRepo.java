/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.repositories;

import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface KvPairRepo {

    KvPair findById(KvIdType idType);

    Iterable<KvPair> findAllById(List<KvIdType> idTypes);

    boolean save(KvPair pair);

    boolean saveAll(List<KvPair> pairs);

    boolean delete(KvPair pair);

    boolean deleteAll(List<KvPair> pairs);
}