/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.configs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
public class PropCfg {

    private static String activeProfile = "prod";

    private static String hdataPrefix = "hdata";

    private static String hkeyPrefix = "hkey";

    private static String eventPrefix = "event";

    private static String queueName = "queue";

    private static String defaultExpire = "180";

    private static Boolean defaultSync = false;
    
    private static Boolean enableMonitor = false;

    private static Long eventLockTimeout = 60L;

    private static Long keyMinCacheTTL = 180L;

    private static Long tableInfoCacheTTL = 3600L;

    private static Long maxCacheSize = 1024L;

    private static Long cacheRecycleSecs = 300L;  // 5 minutes

    private static Boolean enableDbFallback = false;

    private static Long dataMaxCacheTLL = 60L;

    private static String datasourceUrl;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfig() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Value("${spring.profiles.active}")
    public void setActiveProfile(String name) {
        if (name != null && name.length() > 0) {
            activeProfile = name;
        }
    }

    public static String getActiveProfile() {
        return activeProfile;
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

    @Value("${rdbcache.queue_name}")
    public void setQueueName(String name) {
        if (name != null && name.length() > 0) {
            queueName = name.replace("::", "");
        }
    }

    public static String getQueueName() {
        return queueName;
    }

    @Value("${rdbcache.default_expire}")
    public void setDefaultExpire(String expire) {
        defaultExpire = expire;
    }

    public static String getDefaultExpire() {
        return defaultExpire;
    }

    @Value("${rdbcache.default_sync}")
    public void setDefaultSync(Boolean sync) {
        defaultSync = sync;
    }

    public static Boolean getDefaultSync() {
        return defaultSync;
    }

    @Value("${rdbcache.enable_monitor}")
    public void setEnableMonitor(Boolean enable) {
        enableMonitor = enable;
    }

    public static Boolean getEnableMonitor() {
        return enableMonitor;
    }

    @Value("${rdbcache.event_lock_timeout}")
    public void setEventLockTimeout(Long timeout) { eventLockTimeout = timeout; }

    public static Long getEventLockTimeout() { return eventLockTimeout; }

    @Value("${rdbcache.key_min_cache_ttl}")
    public void setKeyInfoCacheTTL(Long ttl) {
        keyMinCacheTTL = ttl;
        if (keyMinCacheTTL < 60l) {
            keyMinCacheTTL = 60l;
        }
    }

    public static Long getKeyMinCacheTTL() {
        return keyMinCacheTTL;
    }

    @Value("${rdbcache.table_info_cache_ttl}")
    public void setTableInfoCacheTTL(Long ttl) {
        tableInfoCacheTTL = ttl;
    }

    public static Long getTableInfoCacheTTL() {
        return tableInfoCacheTTL;
    }

    @Value("${rdbcache.local_cache_max_size}")
    public void setMaxCacheSize(Long maxSize) {
        maxCacheSize = maxSize;
    }

    public static Long getMaxCacheSize() {
        return maxCacheSize;
    }

    @Value("${rdbcache.cache_recycle_secs}")
    public void setCacheRecycleSecs(Long secs) {
        cacheRecycleSecs = secs;
    }

    public static Long getCacheRecycleSecs() {
        return cacheRecycleSecs;
    }

    @Value("${rdbcache.enable_db_fallback}")
    public void setEnableDbFallback(Boolean enable) {
        enableDbFallback = enable;
    }

    public static Boolean getEnableDbFallback() {
        return enableDbFallback;
    }

    @Value("${rdbcache.data_max_cache_ttl}")
    public void setDataMaxCacheTLL(Long tll) {
        dataMaxCacheTLL = tll;
    }

    public static Long getDataMaxCacheTLL() {
        return dataMaxCacheTLL;
    }

    @Value("${spring.datasource.url}")
    public void setDatasourceUrl(String url) {
        if (url != null && url.length() > 0) {
            datasourceUrl = url;
        }
    }

    public static String getDatasourceUrl() {
        return datasourceUrl;
    }

    public static String printConfigurations() {
        return "{"+
          "\"hdataPrefix\": \"" + hdataPrefix + "\", " +
          "\"hkeyPrefix\": \"" + hkeyPrefix + "\", " +
          "\"eventPrefix\": \"" + eventPrefix + "\", " +
          "\"queueName\": \"" + queueName + "\", " +
          "\"defaultExpire\": \"" + defaultExpire + "\", " +
          "\"enableMonitor\": \"" + enableMonitor.toString() + "\", " +
          "\"eventLockTimeout\": \"" + eventLockTimeout.toString() + "\", " +
          "\"keyMinCacheTTL\": \"" + keyMinCacheTTL.toString() + "\", " +
          "\"tableInfoCacheTTL\": \"" + tableInfoCacheTTL.toString() + "\", " +
          "\"maxCacheSize\": \"" + maxCacheSize.toString() + "\", " +
          "\"cacheRecycleSecs\": \"" + cacheRecycleSecs.toString() + "\", " +
          "\"enableDbFallback\": \"" + enableDbFallback.toString() + "\", " +
          "\"dataMaxCacheTLL\": \"" + dataMaxCacheTLL.toString() + "\", "+
          "\"datasourceUrl\": \"" + datasourceUrl + "\""+
           "}";
    }
}
