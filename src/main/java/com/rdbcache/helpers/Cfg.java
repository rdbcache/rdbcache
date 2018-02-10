/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.rdbcache.helpers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

@Component
public class Cfg {

    private static String hdataPrefix = "";

    private static String hkeyPrefix = "";

    private static String eventPrefix = "";

    private static String defaultExpire = "180";

    private static Boolean enableMonitor = false;

    private static Long keyInfoCacheTTL = 180L;

    private static Long tableInfoCacheTTL = 3600L;

    private static Long maxCacheSize = 1024L;

    private static Long recycleSchedule = 10L;  // 10 seconds

    private static Boolean enableDbFallback = false;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfig() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Value("${rdbcache.hdata_prefix}")
    public void setHdataPrefix(String prefix) {
        if (prefix != null && prefix.length() > 0) {
            hdataPrefix = prefix.replace("::", "");
        }
    }

    public static String getHdataPrefix() {
        return hdataPrefix;
    }

    public static String getHdataKey(String key) {
        return hdataPrefix + "::" + key;
    }

    @Value("${rdbcache.hkeys_prefix}")
    public void setHkeyPrefix(String prefix) {
        if (prefix != null && prefix.length() > 0) {
            hkeyPrefix = prefix.replace("::", "");
        }
    }

    public static String getHkeyPrefix() {
        return hkeyPrefix;
    }

    @Value("${rdbcache.event_prefix}")
    public void setEventPrefix(String prefix) {
        if (prefix != null && prefix.length() > 0) {
            eventPrefix = prefix.replace("::", "");
        }
    }

    public static String getEventPrefix() {
        return eventPrefix;
    }

    @Value("${rdbcache.default_expire}")
    public void setDefaultExpire(String expire) {
        defaultExpire = expire;
    }

    public static String getDefaultExpire() {
        return defaultExpire;
    }

    @Value("${rdbcache.enable_monitor}")
    public void setEnableMonitor(Boolean enable) {
        enableMonitor = enable;
    }

    public static Boolean getEnableMonitor() {
        return enableMonitor;
    }

    @Value("${rdbcache.key_info_cache_ttl}")
    public void setKeyInfoCacheTTL(Long ttl) {
        keyInfoCacheTTL = ttl;
    }

    public static Long getKeyInfoCacheTTL() {
        return keyInfoCacheTTL;
    }

    @Value("${rdbcache.table_info_cache_ttl}")
    public void setTableInfoCacheTTL(Long ttl) {
        tableInfoCacheTTL = ttl;
    }

    public static Long getTableInfoCacheTTL() {
        return tableInfoCacheTTL;
    }

    @Value("${rdbcache.local_cache_max_size}")
    public void setMaxCacheSize(Long maxCacheSize) {
        Cfg.maxCacheSize = maxCacheSize;
    }

    public static Long getMaxCacheSize() {
        return maxCacheSize;
    }

    @Value("${rdbcache.local_cache_recycle_schedule}")
    public void setRecycleSchedule(Long recycleSchedule) {
        recycleSchedule = recycleSchedule;
    }

    public static Long getRecycleSchedule() {
        return recycleSchedule;
    }

    @Value("${rdbcache.enable_db_fallback}")
    public void setEnableDbFallback(Boolean enableDbFallback) {
        Cfg.enableDbFallback = enableDbFallback;
    }

    public static Boolean getEnableDbFallback() {
        return enableDbFallback;
    }
}
