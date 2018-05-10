package doitincloud.rdbcache.configs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.servlet.annotation.WebServlet;
import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Autowired
    Environment env;

    @Value("classpath:schema.sql")
    private Resource schemaScript;

    @Value("classpath:data.sql")
    private Resource dataScript;

    @Value("classpath:system-schema.sql")
    private Resource systemSchemaScript;

    @Value("classpath:system-data.sql")
    private Resource systemDataScript;

    @Bean
    public Boolean h2Database() {
        String driver = env.getProperty("system.datasource.driver-class-name");
        if (driver.indexOf(".h2.") > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Bean
    public DatabasePopulator databasePopulator() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(schemaScript);
        populator.addScript(dataScript);
        return populator;
    }

    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(env.getProperty("spring.datasource.driver-class-name"));
        dataSource.setUrl(env.getProperty("spring.datasource.url"));
        dataSource.setUsername(env.getProperty("spring.datasource.username"));
        dataSource.setPassword(env.getProperty("spring.datasource.password"));
        return dataSource;
    }

    @Bean
    public DatabasePopulator systemDatabasePopulator() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        String driver = env.getProperty("system.datasource.driver-class-name");
        populator.addScript(systemSchemaScript);
        populator.addScript(systemDataScript);
        return populator;
    }

    @Bean
    public DataSource systemDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(env.getProperty("system.datasource.driver-class-name"));
        dataSource.setUrl(env.getProperty("system.datasource.url"));
        dataSource.setUsername(env.getProperty("system.datasource.username"));
        dataSource.setPassword(env.getProperty("system.datasource.password"));
        return dataSource;
    }
}
