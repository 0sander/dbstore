package com.cinefms.dbstore.api;

import com.cinefms.dbstore.api.exceptions.DBStoreException;
import com.cinefms.dbstore.query.api.DBStoreQuery;

import java.util.List;

/**
 * Transaction context that provides access to DataStore operations within a transaction.
 * This allows for more fine-grained control over transaction operations.
 */
public interface DBStoreTransactionContext {

    /**
     * Save an object within the transaction
     */
    <T extends DBStoreEntity> T saveObject(T object);

    /**
     * Get an object within the transaction
     */
    <T extends DBStoreEntity> T getObject(Class<T> clazz, String id);

    /**
     * Delete an object within the transaction
     */
    <T extends DBStoreEntity> boolean deleteObject(Class<T> clazz, String id);

    /**
     * Delete an object within the transaction
     */
    <T extends DBStoreEntity> boolean deleteObject(T object);

    /**
     * Delete objects matching query within the transaction
     */
    <T extends DBStoreEntity> boolean deleteObjects(Class<T> type, DBStoreQuery query);

    /**
     * Find objects within the transaction
     */
    <T extends DBStoreEntity> List<T> findObjects(Class<T> clazz, DBStoreQuery query);

    /**
     * Find single object within the transaction
     */
    <T extends DBStoreEntity> T findObject(Class<T> clazz, DBStoreQuery query);

    /**
     * Count objects within the transaction
     */
    <T extends DBStoreEntity> long countObjects(Class<T> clazz, DBStoreQuery query);

    /**
     * Save multiple objects within the transaction
     */
    <T extends DBStoreEntity> List<T> saveObjects(List<T> objects);

    /**
     * Update object fields within the transaction
     */
    <T extends DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, java.util.Map<String, Object> fields);

    /**
     * Update object fields with atomic operations within the transaction
     */
    <T extends DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, List<FieldUpdate> fieldUpdates);

    /**
     * Increment field within the transaction
     */
    <T extends DBStoreEntity> T incrementField(Class<T> clazz, String id, String fieldName, Number increment);

    /**
     * Set field within the transaction
     */
    <T extends DBStoreEntity> T setField(Class<T> clazz, String id, String fieldName, Object value);

    /**
     * Unset field within the transaction
     */
    <T extends DBStoreEntity> T unsetField(Class<T> clazz, String id, String fieldName);

    /**
     * Save binary data within the transaction
     */
    void saveBinary(String bucket, DBStoreBinary binary) throws DBStoreException;

    /**
     * Get binary data within the transaction
     */
    DBStoreBinary getBinary(String bucket, String id) throws DBStoreException;
}
