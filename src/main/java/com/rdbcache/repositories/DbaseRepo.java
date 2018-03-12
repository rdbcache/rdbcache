/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;

public interface DbaseRepo {

    public boolean find(Context context, KeyInfo keyInfo);

    public boolean save(Context context, KeyInfo keyInfo);

    public boolean insert(Context context, KeyInfo keyInfo);

    public boolean update(Context context, KeyInfo keyInfo);

    public boolean delete(Context context, KeyInfo keyInfo);
}
