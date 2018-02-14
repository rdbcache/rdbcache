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

    public boolean findOne(Context context, KeyInfo keyInfo);

    public boolean saveOne(Context context, KeyInfo keyInfo);

    public boolean updateIfExists(Context context, KeyInfo keyInfo);

    public boolean findAll(Context context, KeyInfo keyInfo);

    public boolean saveAll(Context context, KeyInfo keyInfo);

    public boolean findAndSave(Context context, KeyInfo keyInfo);

    public void delete(Context context, KeyInfo keyInfo);

    public void deleteOneCompletely(Context context, KeyInfo keyInfo);
}
