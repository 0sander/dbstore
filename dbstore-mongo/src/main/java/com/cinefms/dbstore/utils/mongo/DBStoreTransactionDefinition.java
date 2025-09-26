package com.cinefms.dbstore.utils.mongo;

import org.springframework.transaction.TransactionDefinition;

/**
 * Custom transaction definition for DBStore operations
 */
public class DBStoreTransactionDefinition implements TransactionDefinition {
    
    private final String database;
    private final int propagationBehavior;
    private final int isolationLevel;
    private final int timeout;
    private final boolean readOnly;
    private final String name;
    
    public DBStoreTransactionDefinition(String database) {
        this(database, PROPAGATION_REQUIRED, ISOLATION_DEFAULT, TIMEOUT_DEFAULT, false, null);
    }
    
    public DBStoreTransactionDefinition(String database, int propagationBehavior) {
        this(database, propagationBehavior, ISOLATION_DEFAULT, TIMEOUT_DEFAULT, false, null);
    }
    
    public DBStoreTransactionDefinition(String database, int propagationBehavior, int isolationLevel) {
        this(database, propagationBehavior, isolationLevel, TIMEOUT_DEFAULT, false, null);
    }
    
    public DBStoreTransactionDefinition(String database, int propagationBehavior, int isolationLevel, int timeout, boolean readOnly, String name) {
        this.database = database;
        this.propagationBehavior = propagationBehavior;
        this.isolationLevel = isolationLevel;
        this.timeout = timeout;
        this.readOnly = readOnly;
        this.name = name;
    }
    
    public String getDatabase() {
        return database;
    }
    
    @Override
    public int getPropagationBehavior() {
        return propagationBehavior;
    }
    
    @Override
    public int getIsolationLevel() {
        return isolationLevel;
    }
    
    @Override
    public int getTimeout() {
        return timeout;
    }
    
    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    /**
     * Builder for creating transaction definitions
     */
    public static class Builder {
        private String database;
        private int propagationBehavior = PROPAGATION_REQUIRED;
        private int isolationLevel = ISOLATION_DEFAULT;
        private int timeout = TIMEOUT_DEFAULT;
        private boolean readOnly = false;
        private String name;
        
        public Builder database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder propagationBehavior(int propagationBehavior) {
            this.propagationBehavior = propagationBehavior;
            return this;
        }
        
        public Builder isolationLevel(int isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }
        
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }
        
        public Builder readOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public DBStoreTransactionDefinition build() {
            return new DBStoreTransactionDefinition(database, propagationBehavior, isolationLevel, timeout, readOnly, name);
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
}
