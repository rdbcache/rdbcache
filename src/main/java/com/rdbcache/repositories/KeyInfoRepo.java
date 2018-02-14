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

    public KeyInfo findOne(Context context);

    public List<KeyInfo> findAll(Context context);

    public void saveOne(Context context, KeyInfo keyInfo);

    public void saveAll(Context context, List<KeyInfo> keyInfos);

    public void deleteOne(Context context);

    public void deleteAll(Context context);
}
