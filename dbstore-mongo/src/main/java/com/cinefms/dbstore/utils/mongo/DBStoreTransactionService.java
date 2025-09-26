package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.DataStore;
import com.cinefms.dbstore.api.DBStoreTransactionContext;
import com.cinefms.dbstore.api.exceptions.DBStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Service for managing DBStore transactions with Spring integration
 */
@Service
public class DBStoreTransactionService {
    
    private final DataStore dataStore;
    
    @Autowired
    public DBStoreTransactionService(DataStore dataStore) {
        this.dataStore = dataStore;
    }
    
    /**
     * Execute operations within a transaction using Spring's @Transactional
     */
    @Transactional
    public <T> T executeInTransaction(String database, Function<DBStoreTransactionContext, T> operations) {
        return dataStore.executeInTransaction(database, operations);
    }
    
    /**
     * Execute operations within a transaction using Spring's @Transactional (void return)
     */
    @Transactional
    public void executeInTransaction(String database, Consumer<DBStoreTransactionContext> operations) {
        dataStore.executeInTransaction(database, operations);
    }
    
    /**
     * Execute operations within a transaction using Spring's @Transactional with default database
     */
    @Transactional
    public <T> T executeInTransaction(Function<DBStoreTransactionContext, T> operations) {
        return dataStore.executeInTransaction(null, operations);
    }
    
    /**
     * Execute operations within a transaction using Spring's @Transactional with default database (void return)
     */
    @Transactional
    public void executeInTransaction(Consumer<DBStoreTransactionContext> operations) {
        dataStore.executeInTransaction(null, operations);
    }
    
    /**
     * Execute operations within a read-only transaction
     */
    @Transactional(readOnly = true)
    public <T> T executeInReadOnlyTransaction(String database, Function<DBStoreTransactionContext, T> operations) {
        return dataStore.executeInTransaction(database, operations);
    }
    
    /**
     * Execute operations within a read-only transaction with default database
     */
    @Transactional(readOnly = true)
    public <T> T executeInReadOnlyTransaction(Function<DBStoreTransactionContext, T> operations) {
        return dataStore.executeInTransaction(null, operations);
    }
    
    /**
     * Check if transactions are supported
     */
    public boolean supportsTransactions() {
        return dataStore.supportsTransactions();
    }
}
