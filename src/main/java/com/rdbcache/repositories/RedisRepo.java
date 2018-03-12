/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;

public interface RedisRepo {

    public boolean ifExits(Context context, KeyInfo keyInfo);

    public boolean find(Context context, KeyInfo keyInfo);

    public boolean save(Context context, KeyInfo keyInfo);

    public boolean updateIfExists(Context context, KeyInfo keyInfo);

    public boolean findAndSave(Context context, KeyInfo keyInfo);

    public void delete(Context context, KeyInfo keyInfo);

    public void deleteCompletely(Context context, KeyInfo keyInfo);
}
