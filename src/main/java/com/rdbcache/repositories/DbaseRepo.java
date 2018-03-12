/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;

public interface DbaseRepo {

    public boolean find(Context context, AnyKey anyKey);

    public boolean save(Context context, AnyKey anyKey);

    public boolean insert(Context context, AnyKey anyKey);

    public boolean update(Context context, AnyKey anyKey);

    public boolean delete(Context context, AnyKey anyKey);
}
