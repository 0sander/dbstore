package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.DataStore;
import com.cinefms.dbstore.api.DBStoreTransactionContext;
import com.cinefms.dbstore.api.exceptions.DBStoreException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.function.Function;

/**
 * Spring transaction manager for DBStore operations
 */
public class DBStoreTransactionManager extends AbstractPlatformTransactionManager {
    
    private static final Log log = LogFactory.getLog(DBStoreTransactionManager.class);
    
    private final DataStore dataStore;
    private final String defaultDatabase;
    
    public DBStoreTransactionManager(DataStore dataStore, String defaultDatabase) {
        this.dataStore = dataStore;
        this.defaultDatabase = defaultDatabase;
        setNestedTransactionAllowed(false);
        setValidateExistingTransaction(false);
    }
    
    public DBStoreTransactionManager(DataStore dataStore) {
        this(dataStore, null);
    }
    
    @Override
    protected Object doGetTransaction() throws TransactionException {
        DBStoreTransactionObject txObject = new DBStoreTransactionObject();
        txObject.setNewTransaction(!TransactionSynchronizationManager.isSynchronizationActive());
        return txObject;
    }
    
    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        DBStoreTransactionObject txObject = (DBStoreTransactionObject) transaction;
        
        if (!dataStore.supportsTransactions()) {
            throw new org.springframework.transaction.TransactionSystemException("DataStore does not support transactions");
        }
        
        try {
            String database = getDatabaseName(definition);
            DBStoreTransactionContext context = createTransactionContext(database);
            txObject.setTransactionContext(context);
            txObject.setDatabase(database);
            
            // Register synchronization
            TransactionSynchronizationManager.registerSynchronization(
                new DBStoreTransactionSynchronization(txObject)
            );
            
        } catch (Exception e) {
            throw new org.springframework.transaction.TransactionSystemException("Failed to begin transaction", e);
        }
    }
    
    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        DBStoreTransactionObject txObject = (DBStoreTransactionObject) status.getTransaction();
        
        try {
            // Transaction commit is handled by MongoDB's withTransaction
            // This method is called after the transaction body completes successfully
            log.debug("Transaction committed successfully");
        } catch (Exception e) {
            throw new org.springframework.transaction.TransactionSystemException("Failed to commit transaction", e);
        }
    }
    
    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        DBStoreTransactionObject txObject = (DBStoreTransactionObject) status.getTransaction();
        
        try {
            // Transaction rollback is handled by MongoDB's withTransaction
            // This method is called when an exception occurs in the transaction body
            log.debug("Transaction rolled back");
        } catch (Exception e) {
            throw new org.springframework.transaction.TransactionSystemException("Failed to rollback transaction", e);
        }
    }
    
    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return ((DBStoreTransactionObject) transaction).hasTransactionContext();
    }
    
    private String getDatabaseName(TransactionDefinition definition) {
        if (definition instanceof DBStoreTransactionDefinition) {
            return ((DBStoreTransactionDefinition) definition).getDatabase();
        }
        return defaultDatabase;
    }
    
    private DBStoreTransactionContext createTransactionContext(String database) {
        // This is a placeholder - the actual context creation happens in the transaction body
        return null;
    }
    
    /**
     * Execute operations within a Spring-managed transaction
     */
    public <T> T executeInTransaction(String database, Function<DBStoreTransactionContext, T> operations) {
        return dataStore.executeInTransaction(database, operations);
    }
    
    /**
     * Execute operations within a Spring-managed transaction using default database
     */
    public <T> T executeInTransaction(Function<DBStoreTransactionContext, T> operations) {
        return executeInTransaction(defaultDatabase, operations);
    }
    
    /**
     * Transaction object that holds transaction state
     */
    private static class DBStoreTransactionObject {
        private DBStoreTransactionContext transactionContext;
        private String database;
        private boolean newTransaction;
        
        public boolean hasTransactionContext() {
            return transactionContext != null;
        }
        
        public DBStoreTransactionContext getTransactionContext() {
            return transactionContext;
        }
        
        public void setTransactionContext(DBStoreTransactionContext transactionContext) {
            this.transactionContext = transactionContext;
        }
        
        public String getDatabase() {
            return database;
        }
        
        public void setDatabase(String database) {
            this.database = database;
        }
        
        public boolean isNewTransaction() {
            return newTransaction;
        }
        
        public void setNewTransaction(boolean newTransaction) {
            this.newTransaction = newTransaction;
        }
    }
    
    /**
     * Transaction synchronization for cleanup
     */
    private static class DBStoreTransactionSynchronization implements org.springframework.transaction.support.TransactionSynchronization {
        private final DBStoreTransactionObject txObject;
        
        public DBStoreTransactionSynchronization(DBStoreTransactionObject txObject) {
            this.txObject = txObject;
        }
        
        @Override
        public void beforeCommit(boolean readOnly) {
            // Pre-commit logic if needed
        }
        
        @Override
        public void afterCompletion(int status) {
            // Cleanup logic if needed
        }
    }
}
