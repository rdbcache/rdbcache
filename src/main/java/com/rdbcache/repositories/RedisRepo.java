/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.AnyKey;
import com.rdbcache.helpers.Context;

public interface RedisRepo {

    public boolean ifExits(Context context, AnyKey anyKey);

    public boolean find(Context context, AnyKey anyKey);

    public boolean save(Context context, AnyKey anyKey);

    public boolean updateIfExists(Context context, AnyKey anyKey);

    public boolean findAndSave(Context context, AnyKey anyKey);

    public void delete(Context context, AnyKey anyKey);

    public void deleteCompletely(Context context, AnyKey anyKey);
}
