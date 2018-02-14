/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */
package com.rdbcache.configs;

import com.rdbcache.helpers.AppCtxAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppCtxConfig {

    @Bean
    public AppCtxAware appCtxAware() {
        return new AppCtxAware();
    }
}
