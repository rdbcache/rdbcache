/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;
import com.rdbcache.helpers.KvPairs;

public interface RedisRepo {

    public boolean ifExits(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean find(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean save(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean updateIfExists(Context context, KvPairs pairs, AnyKey anyKey);

    public boolean findAndSave(Context context, KvPairs pairs, AnyKey anyKey);

    public void delete(Context context, KvPairs pairs, AnyKey anyKey, boolean dbOps);
}
