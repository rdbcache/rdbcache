package doitincloud.rdbcache.configs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.DatabasePopulator;

import javax.sql.DataSource;

@Configuration
public class JdbcTemplateConfig {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DataSource systemDataSource;

    @Autowired
    private DatabasePopulator databasePopulator;

    @Autowired
    private DatabasePopulator systemDatabasePopulator;

    @Bean
    public DataSourceInitializer dataSourceInitializer() {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);
        initializer.setEnabled(true);
        return initializer;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        return template;
    }

    @Bean
    public DataSourceInitializer systemDataSourceInitializer() {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(systemDataSource);
        initializer.setDatabasePopulator(systemDatabasePopulator);
        initializer.setEnabled(true);
        return initializer;
    }

    @Bean
    public JdbcTemplate systemJdbcTemplate() {
        JdbcTemplate template = new JdbcTemplate(systemDataSource);
        return template;
    }
}
