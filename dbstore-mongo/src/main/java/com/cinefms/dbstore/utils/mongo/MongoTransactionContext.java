package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.DBStoreBinary;
import com.cinefms.dbstore.api.DBStoreEntity;
import com.cinefms.dbstore.api.DBStoreTransactionContext;
import com.cinefms.dbstore.api.FieldUpdate;
import com.cinefms.dbstore.api.exceptions.DBStoreException;
import com.cinefms.dbstore.query.api.DBStoreQuery;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import org.mongojack.JacksonMongoCollection;

import java.util.List;
import java.util.Map;

/**
 * MongoDB implementation of transaction context
 */
public class MongoTransactionContext implements DBStoreTransactionContext {
    
    private final AMongoDataStore dataStore;
    private final String db;
    private final ClientSession session;
    private final MongoDatabase mongoDatabase;
    
    public MongoTransactionContext(AMongoDataStore dataStore, String db, ClientSession session) {
        this.dataStore = dataStore;
        this.db = db;
        this.session = session;
        this.mongoDatabase = dataStore.getDB(db);
    }
    
    @Override
    public <T extends DBStoreEntity> T saveObject(T object) {
        return dataStore.saveObjectInTransaction(db, object, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T getObject(Class<T> clazz, String id) {
        return dataStore.getObjectInTransaction(db, clazz, id, session);
    }
    
    @Override
    public <T extends DBStoreEntity> boolean deleteObject(Class<T> clazz, String id) {
        return dataStore.deleteObjectInTransaction(db, clazz, id, session);
    }
    
    @Override
    public <T extends DBStoreEntity> boolean deleteObject(T object) {
        return dataStore.deleteObjectInTransaction(db, object, session);
    }
    
    @Override
    public <T extends DBStoreEntity> boolean deleteObjects(Class<T> type, DBStoreQuery query) {
        return dataStore.deleteObjectsInTransaction(db, type, query, session);
    }
    
    @Override
    public <T extends DBStoreEntity> List<T> findObjects(Class<T> clazz, DBStoreQuery query) {
        return dataStore.findObjectsInTransaction(db, clazz, query, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T findObject(Class<T> clazz, DBStoreQuery query) {
        return dataStore.findObjectInTransaction(db, clazz, query, session);
    }
    
    @Override
    public <T extends DBStoreEntity> long countObjects(Class<T> clazz, DBStoreQuery query) {
        return dataStore.countObjectsInTransaction(db, clazz, query, session);
    }
    
    @Override
    public <T extends DBStoreEntity> List<T> saveObjects(List<T> objects) {
        return dataStore.saveObjectsInTransaction(db, objects, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, Map<String, Object> fields) {
        return dataStore.updateObjectFieldsInTransaction(db, clazz, id, fields, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, List<FieldUpdate> fieldUpdates) {
        return dataStore.updateObjectFieldsInTransaction(db, clazz, id, fieldUpdates, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T incrementField(Class<T> clazz, String id, String fieldName, Number increment) {
        return dataStore.incrementFieldInTransaction(db, clazz, id, fieldName, increment, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T setField(Class<T> clazz, String id, String fieldName, Object value) {
        return dataStore.setFieldInTransaction(db, clazz, id, fieldName, value, session);
    }
    
    @Override
    public <T extends DBStoreEntity> T unsetField(Class<T> clazz, String id, String fieldName) {
        return dataStore.unsetFieldInTransaction(db, clazz, id, fieldName, session);
    }
    
    @Override
    public void saveBinary(String bucket, DBStoreBinary binary) throws DBStoreException {
        dataStore.saveBinaryInTransaction(db, bucket, binary, session);
    }
    
    @Override
    public DBStoreBinary getBinary(String bucket, String id) throws DBStoreException {
        return dataStore.getBinaryInTransaction(db, bucket, id, session);
    }
    
    /**
     * Get the underlying MongoDB session for advanced operations
     */
    public ClientSession getSession() {
        return session;
    }
    
    /**
     * Get the underlying MongoDB database for advanced operations
     */
    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }
}
