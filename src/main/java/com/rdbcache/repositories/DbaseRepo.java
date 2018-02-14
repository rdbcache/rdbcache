/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;

public interface DbaseRepo {

    public boolean findOne(Context context, KeyInfo keyInfo);

    public boolean findAll(Context context, KeyInfo keyInfo);

    public boolean saveOne(Context context, KeyInfo keyInfo);

    public boolean saveAll(Context context, KeyInfo keyInfo);

    public boolean insertOne(Context context, KeyInfo keyInfo);

    public boolean insertAll(Context context, KeyInfo keyInfo);

    public boolean updateOne(Context context, KeyInfo keyInfo);

    public boolean deleteOne(Context context, KeyInfo keyInfo);
}
