/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud;

import doitincloud.rdbcache.configs.AppCtx;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

@SpringBootApplication
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

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