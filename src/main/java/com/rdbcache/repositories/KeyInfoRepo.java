/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.repositories;

import com.rdbcache.helpers.Context;
import com.rdbcache.models.KeyInfo;

import java.util.List;

public interface KeyInfoRepo {

    public boolean find(Context context, KeyInfo keyInfo);

    public boolean find(Context context, List<KeyInfo> keyInfos);

    public boolean save(Context context, KeyInfo keyInfo);

    public boolean save(Context context, List<KeyInfo> keyInfos);

    public void delete(Context context, boolean dbOps);
}
