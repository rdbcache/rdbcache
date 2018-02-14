/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;
import org.springframework.data.repository.CrudRepository;

public interface KvPairRepo extends CrudRepository<KvPair, KvIdType> {
}
