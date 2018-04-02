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

@Repository
public interface KvPairRepo extends CrudRepository<KvPair, KvIdType> {
}
