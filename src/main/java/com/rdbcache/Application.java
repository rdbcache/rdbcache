/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache;

import com.rdbcache.configs.AppCtx;
import com.rdbcache.helpers.PropCfg;
import com.rdbcache.helpers.VersionInfo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.PrintStream;

@SpringBootApplication
public class Application implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    /*
    @Value("${spring.redis.url}")
    private String redisUrl;

    @Autowired
    private JedisConnectionFactory redisConnectionFactory;

    @Value("${spring.datasource.url}")
    private String dbaseUrl;

    @Autowired
    DataSource dataSource;

    @Bean
    JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource);
    }
    */

    @Override
    public void run(ApplicationArguments args) throws Exception {

        /*
        JedisConnectionFactory jedisConnectionFactory = (JedisConnectionFactory) redisConnectionFactory;
        HikariDataSource hikariDataSource = (HikariDataSource)dataSource;

        String message = "";

        message += "redis url: " + redisUrl + ", pool size: " + jedisConnectionFactory.getPoolConfig().getMaxTotal();
        message += "\ndatabase url: " + dbaseUrl + ", pool size: " + hikariDataSource.getMaximumPoolSize();
        message += "\nconfigurations: " + PropCfg.printConfigurations();

        System.out.println(message);
        */
    }

    public static void main(String[] args) {

        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-v") || args[i].equals("--version")) {
                    System.out.println("\n" + AppCtx.getVersionInfo().getFullInfo() + "\n");
                    return;
                }
            }
        }

        SpringApplication app = new SpringApplication(Application.class);

        app.setBanner(new Banner() {
            @Override
            public void printBanner(Environment environment, Class<?> aClass, PrintStream printStream) {
                printStream.println("\n" + AppCtx.getVersionInfo().getFullInfo() + "\n");
            }
        });

        ConfigurableApplicationContext context = app.run(args);
    }
}