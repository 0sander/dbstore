package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.DataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring configuration for DBStore transaction support
 */
@Configuration
@EnableTransactionManagement
public class DBStoreTransactionConfiguration {
    
    @Autowired
    private DataStore dataStore;
    
    @Bean
    public PlatformTransactionManager dbStoreTransactionManager() {
        return new DBStoreTransactionManager(dataStore);
    }
    
    @Bean
    public TransactionTemplate dbStoreTransactionTemplate() {
        return new TransactionTemplate(dbStoreTransactionManager());
    }
    
    @Bean
    public DBStoreTransactionService dbStoreTransactionService() {
        return new DBStoreTransactionService(dataStore);
    }
}
