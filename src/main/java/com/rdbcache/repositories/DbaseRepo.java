/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;
import org.springframework.stereotype.Repository;

@Repository
public interface DbaseRepo {

    public boolean find(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean save(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean insert(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean update(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean delete(Context context, KvPairs pairs, AnyKey anyKey);
}
