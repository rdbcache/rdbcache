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

package com.rdbcache;

import com.rdbcache.helpers.AppCtx;
import com.rdbcache.helpers.VersionInfo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintStream;

@SpringBootApplication
public class Application implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static VersionInfo versionInfo;

    @Value("${spring.redis.url}")
    private String redisUrl;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Value("${spring.datasource.url}")
    private String dbaseUrl;

    @Autowired
    DataSource dataSource;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        AppCtx.getDbaseOps().setup();

        JedisConnectionFactory jedisConnectionFactory = (JedisConnectionFactory) redisConnectionFactory;
        HikariDataSource hikariDataSource = (HikariDataSource)dataSource;

        String messsage = "";

        messsage += "\nredis url: " + redisUrl + ", pool size: " + jedisConnectionFactory.getPoolConfig().getMaxTotal();
        messsage += "\ndatabase url: " + dbaseUrl + ", pool size: " + hikariDataSource.getMaximumPoolSize();

        LOGGER.info(messsage);

        AppCtx.getLocalCache().start();
        AppCtx.getTaskQueue().start();
    }

    public static void main(String[] args) {

        versionInfo = new VersionInfo();

        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-v") || args[i].equals("--version")) {
                    System.out.println("\n" + versionInfo.getFullInfo() + "\n");
                    return;
                }
            }
        }

        SpringApplication app = new SpringApplication(Application.class);

        app.setBanner(new Banner() {
            @Override
            public void printBanner(Environment environment, Class<?> aClass, PrintStream printStream) {
                printStream.println("\n" + versionInfo.getFullInfo() + "\n");
            }
        });

        ConfigurableApplicationContext context = app.run(args);
    }
}