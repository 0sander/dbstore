package com.cinefms.dbstore.api;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.cinefms.dbstore.api.exceptions.DBStoreException;
import com.cinefms.dbstore.query.api.DBStoreQuery;

public interface DataStore {

	<T extends DBStoreEntity> T saveObject(String db, T object);

	<T extends DBStoreEntity> boolean deleteObject(String db, Class<T> clazz, String id);

	<T extends DBStoreEntity> boolean deleteObject(String db, T object);

	<T extends DBStoreEntity> boolean deleteObjects(String db, Class<T> type, DBStoreQuery query);

	<T extends DBStoreEntity> T getObject(String db, Class<T> clazz, String id);

	<T extends DBStoreEntity> List<T> findObjects(String db, Class<T> clazz, DBStoreQuery query);

	<T extends DBStoreEntity> long countObjects(String db, Class<T> clazz, DBStoreQuery query);

	<T extends DBStoreEntity> T findObject(String db, Class<T> clazz, DBStoreQuery query);

	void addListener(DBStoreListener<?> listener);
	
	public void saveBinary(String dbName, String bucket, DBStoreBinary binary) throws DBStoreException;
	
	public DBStoreBinary getBinary(String dbName, String bucket, String id) throws DBStoreException;

	<T extends DBStoreEntity> List<T> saveObjects(String db, List<T> objects);

	<T extends DBStoreEntity> T updateObjectFields(String db, Class<T> clazz, String id, Map<String, Object> fields);

	<T extends DBStoreEntity> T updateObjectFields(String db, Class<T> clazz, String id, List<FieldUpdate> fieldUpdates);

	// Convenience methods for common atomic operations
	<T extends DBStoreEntity> T incrementField(String db, Class<T> clazz, String id, String fieldName, Number increment);

	<T extends DBStoreEntity> T setField(String db, Class<T> clazz, String id, String fieldName, Object value);

	<T extends DBStoreEntity> T unsetField(String db, Class<T> clazz, String id, String fieldName);

	// Transaction support methods
	
	/**
	 * Execute operations within a transaction
	 * @param db Database name
	 * @param operations Operations to execute within the transaction
	 * @return Result of the operations
	 * @throws DBStoreException if transaction fails
	 */
	<T> T executeInTransaction(String db, Supplier<T> operations) throws DBStoreException;
	
	/**
	 * Execute operations within a transaction (void return)
	 * @param db Database name
	 * @param operations Operations to execute within the transaction
	 * @throws DBStoreException if transaction fails
	 */
	void executeInTransaction(String db, Runnable operations) throws DBStoreException;
	
	/**
	 * Check if transactions are supported by this DataStore implementation
	 * @return true if transactions are supported
	 */
	boolean supportsTransactions();
	
	/**
	 * Execute operations within a transaction with access to transaction context
	 * @param db Database name
	 * @param operations Operations to execute with transaction context
	 * @return Result of the operations
	 * @throws DBStoreException if transaction fails
	 */
	<T> T executeInTransaction(String db, java.util.function.Function<DBStoreTransactionContext, T> operations) throws DBStoreException;
	
	/**
	 * Execute operations within a transaction with access to transaction context (void return)
	 * @param db Database name
	 * @param operations Operations to execute with transaction context
	 * @throws DBStoreException if transaction fails
	 */
	void executeInTransaction(String db, java.util.function.Consumer<DBStoreTransactionContext> operations) throws DBStoreException;

}
