/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.models.StopWatch;
import org.springframework.data.repository.CrudRepository;

public interface StopWatchRepo extends CrudRepository<StopWatch, Long> {
}
