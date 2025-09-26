package com.cinefms.dbstore.utils.mongo;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.mongojack.JacksonMongoCollection;
import org.springframework.beans.factory.annotation.Autowired;

import com.cinefms.dbstore.api.DBStoreBinary;
import com.cinefms.dbstore.api.impl.BasicBinary;
import com.cinefms.dbstore.api.DBStoreEntity;
import com.cinefms.dbstore.api.DBStoreListener;
import com.cinefms.dbstore.api.DataStore;
import com.cinefms.dbstore.api.DBStoreTransactionContext;
import com.cinefms.dbstore.api.FieldUpdate;
import com.cinefms.dbstore.api.UpdateOperation;
import com.cinefms.dbstore.api.annotations.Index;
import com.cinefms.dbstore.api.annotations.Indexes;
import com.cinefms.dbstore.api.annotations.Write;
import com.cinefms.dbstore.api.annotations.WriteMode;
import com.cinefms.dbstore.api.exceptions.DBStoreException;
import com.cinefms.dbstore.api.impl.BasicBinary;
import com.cinefms.dbstore.api.impl.IOUtils;
import com.cinefms.dbstore.query.api.DBStoreQuery;
import com.cinefms.dbstore.query.api.impl.BasicQuery;
import com.cinefms.dbstore.query.mongo.QueryMongojackTranslator;
import com.cinefms.dbstore.utils.mongo.util.CollectionNamingStrategy;
import com.cinefms.dbstore.utils.mongo.util.SimpleCollectionNamingStrategy;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.MongoClient;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.result.DeleteResult;
import java.util.HashMap;

public abstract class AMongoDataStore implements DataStore {

	protected static final Log log = LogFactory.getLog(AMongoDataStore.class);
	private final Map<String, JacksonMongoCollection<?>> collections = new HashMap<>();
	private final Map<String, List<DBStoreListener<?>>> listenerMap = new HashMap<>();
	private final QueryMongojackTranslator fqtl = new QueryMongojackTranslator();

	@Autowired(required = false)
	private List<DBStoreListener<?>> listeners = new ArrayList<>();
	private CollectionNamingStrategy collectionNamingStrategy = new SimpleCollectionNamingStrategy();

	
	private Map<String, GridFSBucket> buckets = new HashMap<String, GridFSBucket>();
	
	public abstract MongoDatabase getDB(String db);
	
	public abstract MongoClient getMongoClient();

	private <T> MongoCollection<T> initializeCollection(MongoDatabase db, Class<T> clazz) {
		String collectionName = getCollectionName(clazz);
		MongoCollection<T> dbc = db.getCollection(collectionName, clazz);

		if (clazz.getAnnotation(Indexes.class) != null) {
			for (Index i : clazz.getAnnotation(Indexes.class).value()) {

				Bson idx = com.mongodb.client.model.Indexes.ascending(i.fields());

				IndexOptions options = new IndexOptions();
				options.unique(i.unique());

				log.debug(" === CREATING INDEX: " + idx + " ==== ");
				dbc.createIndex(idx, options);
			}
		}

		return dbc;
	}

	@SuppressWarnings("unchecked")
	private <T> JacksonMongoCollection<T> getCollection(String db, Class<T> clazz) {
		String collectionName = getCollectionName(clazz);
		String key = db + ":" + collectionName;
		log.debug(" == DB        : " + db);
		log.debug(" == Collection: " + collectionName);

		try {
			JacksonMongoCollection<T> out = (JacksonMongoCollection<T>) collections.get(key);

			if (out == null) {
				log.debug("============================================================");
				log.debug("==");
				log.debug("== DB COLLECTION NOT CREATED .... (creating...) ");
				log.debug("==");
				log.debug("==  CLAZZ IS: " + clazz.getCanonicalName());
				log.debug("== DBNAME IS: " + db);
				MongoDatabase d = getDB(db);
				log.debug("==     DB IS: " + d);
				MongoCollection<T> dbc = initializeCollection(d, clazz);
				log.debug("==    DBC IS: " + dbc);
				out = JacksonMongoCollection.builder().build(dbc, clazz, UuidRepresentation.JAVA_LEGACY);
				if (clazz.getAnnotation(Write.class) != null && clazz.getAnnotation(Write.class).value() == WriteMode.FAST) {
					out.withWriteConcern(WriteConcern.UNACKNOWLEDGED);
				}
				collections.put(key, out);
				log.debug("==");
				log.debug("============================================================");
			}

			return out;

		} catch (Exception e) {
			throw new RuntimeException("Unable to obtain collection '" + collectionName + "'", e);
		}
	}

	private List<DBStoreListener<?>> getListeners(Class<? extends DBStoreEntity> clazz) {
		List<DBStoreListener<?>> out = listenerMap.get(clazz.getCanonicalName());

		if (out == null) {
			out = new ArrayList<>();

			if (listeners != null) {
				for (DBStoreListener<?> l : listeners) {
					if (l.supports(clazz)) {
						log.debug("listeners on " + clazz + ": " + l.getClass() + " supports");
						out.add(l);

					} else {
						log.debug("listeners on " + clazz + ": " + l.getClass() + " does not support");
					}
				}
			}

			listenerMap.put(clazz.getCanonicalName(), out);
		}

		log.debug("listeners on " + clazz + ": " + out.size());
		return out;
	}

	@Override
	public <T extends DBStoreEntity> T findObject(String db, Class<T> clazz, DBStoreQuery query) {
		return getCollection(db, clazz)
				.find(fqtl.translate(query))
				.sort(fqtl.translateOrderBy(query))
				.limit(1)
				.first();
	}

	@Override
	public <T extends DBStoreEntity> long countObjects(String db, Class<T> clazz, DBStoreQuery query) {
		return getCollection(db, clazz).countDocuments(fqtl.translate(query));
	}

	@Override
	public <T extends DBStoreEntity> List<T> findObjects(String db, Class<T> clazz, DBStoreQuery query) {
		FindIterable<T> f = getCollection(db, clazz)
				.find(fqtl.translate(query))
				.sort(fqtl.translateOrderBy(query));

		int skip = 0;
		int max = 0;
		
		if (query != null) {
			skip = query.getStart();
			max = query.getMax();
		}
		
		if (skip > 0) {
			f = f.skip(skip);
		}

		if (max > 0 && max < Integer.MAX_VALUE) {
			f = f.limit(max);
		}

		log.debug(" ---> LIMIT (" + skip + ":" + max + ")");

		List<T> out = new LinkedList<>();
		f.forEach(out::add);

		log.debug("-- db query: found " + out.size() + " matches for query (" + clazz.getCanonicalName() + ":" + query + ")");

		return out;
	}

	@Override
	public <T extends DBStoreEntity> T getObject(String db, Class<T> clazz, String id) {
		return getCollection(db, clazz).findOneById(id);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends DBStoreEntity> boolean deleteObject(String db, T object) {
		return object != null && deleteObject(db, object.getClass(), object.getId());
	}

	@Override
	public <T extends DBStoreEntity> boolean deleteObject(String db, Class<T> clazz, String id) {
		return id != null && deleteObjects(db, clazz, BasicQuery.createQuery().eq("_id", id));
	}

	@Override
	public <T extends DBStoreEntity> boolean deleteObjects(String db, Class<T> clazz, DBStoreQuery query) {
		List<DBStoreListener<?>> entityListeners = getListeners(clazz);

		boolean anyDeleted = false;

		for (T object : getCollection(db, clazz).find(fqtl.translate(query))) {
			if (object == null || object.getId() == null) {
				continue;
			}

			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'beforeDelete' for: " + clazz + " / " + object.getId());
				listener.beforeDelete(db, object);
			}

			try {
				getCollection(db, clazz).removeById(object.getId());

				for (DBStoreListener listener : entityListeners) {
					log.debug("firing 'delete' for: " + clazz + " / " + object.getId());
					listener.deleted(db, object);
				}


				anyDeleted = true;

			} catch (Exception e) {
				throw new DBStoreException(e);
			}
		}

		return anyDeleted;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends DBStoreEntity> List<T> saveObjects(String db, List<T> objects) {
		if (objects.isEmpty()) return objects;

		Class<T> clazz = (Class<T>) objects.stream().findFirst().map(Object::getClass).orElse(null);

		List<DBStoreListener<?>> entityListeners = getListeners(clazz);
		List<T> out = new ArrayList<>(objects.size());

		for (T object : objects) {
			log.debug(clazz + " / saving object: " + object.getId() + ", notifying " + entityListeners.size() + " listeners");

			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'beforeSave' for: " + clazz + " / " + object.getId());
				listener.beforeSave(db, object);
			}

			JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, object.getClass());

			T old = null;
			if (object.getId() != null) {
				old = coll.findOneById(object.getId());
			} else {
				String id = object.createId();
				if (id == null) {
					id = ObjectId.get().toString();
				}
				object.setId(id);
			}

			if (old != null) {
				if (!needsUpdate(old, object)) {
					log.debug("no change, returning");
					out.add(object);
					continue;
				}

				coll.replaceOne(Filters.eq("_id", old.getId()), object);
			} else {
				coll.save(object);
			}

			object = getObject(db, clazz, object.getId());

			for (DBStoreListener listener : entityListeners) {
				if (old != null) {
					log.debug("firing 'updated' for: " + out.getClass() + " / " + object.getId() + " / " + listener.getClass());
					listener.updated(db, old, object);

				} else {
					log.debug("firing 'created' for: " + out.getClass() + " / " + object.getId() + " / " + listener.getClass());
					listener.created(db, object);
				}
			}

			out.add(object);
		}

		return out;
	}

	public <T extends DBStoreEntity> T saveObject(String db, T object) {
		return saveObjects(db, Collections.singletonList(object))
				.stream()
				.findFirst()
				.orElse(null);
	}

	public <T> boolean needsUpdate(T old, T object) {
		return true;
	}

	@Override
	public void addListener(DBStoreListener<?> listener) {
		this.listeners.add(listener);
	}

	public String getCollectionName(Class<?> clazz) {
		return collectionNamingStrategy.getCollectionName(clazz);
	}

	public CollectionNamingStrategy getCollectionNamingStrategy() {
		return collectionNamingStrategy;
	}

	public void setCollectionNamingStrategy(CollectionNamingStrategy collectionNamingStrategy) {
		this.collectionNamingStrategy = collectionNamingStrategy;
	}
	
	private GridFSBucket getBucket(String db, String bucket) {
		GridFSBucket out = buckets.get(bucket);
		if (out == null) {
			out = GridFSBuckets.create(getDB(db), bucket);
			buckets.put(bucket, out);
		}
		return out;
	}	

	@Override
	public DBStoreBinary getBinary(String dbName, String bucket, String id) throws DBStoreException {
		GridFSBucket b = getBucket(dbName,bucket);
		try {

			GridFSFile f   = b.find(Filters.or(Filters.eq("metadata.ID", id), Filters.eq("filename", id))).first();
			
			if (f == null) {
				return null;
			}
			
			InputStream is = b.openDownloadStream(f.getId());
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copy(is, baos);
			baos.flush();
			
			HashMap<String, Object> md = new HashMap<String, Object>();
			Document metadata = f.getMetadata();
			for(String s : metadata.keySet()) {
				md.put(s, metadata.get(s));
			}
			
			BasicBinary bb = new BasicBinary(id, baos.toByteArray(),md);
			
			return bb;
			
		} catch (Exception e) {
			throw new DBStoreException("error loading binary", e);
		}
		
	}
	
	@Override
	public void saveBinary(String dbName, String bucket, DBStoreBinary binary) throws DBStoreException {
		GridFSBucket b = getBucket(dbName,bucket);
		Document md = new Document();
		for (Map.Entry<String, Object> e : binary.getMetaData().entrySet()) {
			md.put(e.getKey(), e.getValue());
		}
		md.put("ID", binary.getId());
		GridFSUploadOptions o = new GridFSUploadOptions().chunkSizeBytes(1024).metadata(md);
        b.uploadFromStream(binary.getId(),binary.getInputStream(),o);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends DBStoreEntity> T updateObjectFields(String db, Class<T> clazz, String id, Map<String, Object> fields) {
		if (id == null || fields == null || fields.isEmpty()) {
			return null;
		}

		List<DBStoreListener<?>> entityListeners = getListeners(clazz);
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);

		// Get the old object for listener notifications
		T old = coll.findOneById(id);
		if (old == null) {
			log.debug("No object found with id: " + id + " in class: " + clazz.getSimpleName());
			return null;
		}

		// Fire beforeSave listeners
		for (DBStoreListener listener : entityListeners) {
			log.debug("firing 'beforeSave' for: " + clazz + " / " + id);
			listener.beforeSave(db, old);
		}

		try {
			// Build the update document
			List<Bson> updateOperations = new ArrayList<>();
			for (Map.Entry<String, Object> entry : fields.entrySet()) {
				updateOperations.add(Updates.set(entry.getKey(), entry.getValue()));
			}

			// Perform atomic update
			Bson updateDoc = Updates.combine(updateOperations);
			UpdateOptions options = new UpdateOptions();
			
			// Use updateOne for atomic operation
			coll.updateOne(Filters.eq("_id", id), updateDoc, options);

			// Get the updated object
			T updated = getObject(db, clazz, id);

			// Fire updated listeners
			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'updated' for: " + clazz + " / " + id + " / " + listener.getClass());
				listener.updated(db, old, updated);
			}

			return updated;

		} catch (Exception e) {
			log.error("Error updating fields for object with id: " + id, e);
			throw new DBStoreException("Error updating fields for object with id: " + id, e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends DBStoreEntity> T updateObjectFields(String db, Class<T> clazz, String id, List<FieldUpdate> fieldUpdates) {
		if (id == null || fieldUpdates == null || fieldUpdates.isEmpty()) {
			return null;
		}

		List<DBStoreListener<?>> entityListeners = getListeners(clazz);
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);

		// Get the old object for listener notifications
		T old = coll.findOneById(id);
		if (old == null) {
			log.debug("No object found with id: " + id + " in class: " + clazz.getSimpleName());
			return null;
		}

		// Fire beforeSave listeners
		for (DBStoreListener listener : entityListeners) {
			log.debug("firing 'beforeSave' for: " + clazz + " / " + id);
			listener.beforeSave(db, old);
		}

		try {
			// Build the update document using different MongoDB operators
			List<Bson> updateOperations = new ArrayList<>();
			for (FieldUpdate fieldUpdate : fieldUpdates) {
				Bson operation = createUpdateOperation(fieldUpdate);
				if (operation != null) {
					updateOperations.add(operation);
				}
			}

			if (updateOperations.isEmpty()) {
				log.debug("No valid update operations found");
				return old;
			}

			// Perform atomic update
			Bson updateDoc = Updates.combine(updateOperations);
			UpdateOptions options = new UpdateOptions();
			
			// Use updateOne for atomic operation
			coll.updateOne(Filters.eq("_id", id), updateDoc, options);

			// Get the updated object
			T updated = getObject(db, clazz, id);

			// Fire updated listeners
			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'updated' for: " + clazz + " / " + id + " / " + listener.getClass());
				listener.updated(db, old, updated);
			}

			return updated;

		} catch (Exception e) {
			log.error("Error updating fields for object with id: " + id, e);
			throw new DBStoreException("Error updating fields for object with id: " + id, e);
		}
	}

	/**
	 * Creates a MongoDB update operation based on the FieldUpdate type
	 */
	private Bson createUpdateOperation(FieldUpdate fieldUpdate) {
		String fieldName = fieldUpdate.getFieldName();
		UpdateOperation operation = fieldUpdate.getOperation();
		Object value = fieldUpdate.getValue();

		switch (operation) {
			case SET:
				return Updates.set(fieldName, value);
			case INC:
				return Updates.inc(fieldName, (Number) value);
			case UNSET:
				return Updates.unset(fieldName);
			case PUSH:
				return Updates.push(fieldName, value);
			case PULL:
				return Updates.pull(fieldName, value);
			case ADD_TO_SET:
				return Updates.addToSet(fieldName, value);
			case MUL:
				return Updates.mul(fieldName, (Number) value);
			case MIN:
				return Updates.min(fieldName, value);
			case MAX:
				return Updates.max(fieldName, value);
			case RENAME:
				return Updates.rename(fieldName, (String) value);
			case SET_ON_INSERT:
				return Updates.setOnInsert(fieldName, value);
			default:
				log.warn("Unknown update operation: " + operation);
				return null;
		}
	}

	@Override
	public <T extends DBStoreEntity> T incrementField(String db, Class<T> clazz, String id, String fieldName, Number increment) {
		return updateObjectFields(db, clazz, id, Collections.singletonList(FieldUpdate.inc(fieldName, increment)));
	}

	@Override
	public <T extends DBStoreEntity> T setField(String db, Class<T> clazz, String id, String fieldName, Object value) {
		return updateObjectFields(db, clazz, id, Collections.singletonList(FieldUpdate.set(fieldName, value)));
	}

	@Override
	public <T extends DBStoreEntity> T unsetField(String db, Class<T> clazz, String id, String fieldName) {
		return updateObjectFields(db, clazz, id, Collections.singletonList(FieldUpdate.unset(fieldName)));
	}
	
	// Transaction support implementation
	
	@Override
	public boolean supportsTransactions() {
		return true;
	}
	
	@Override
	public <T> T executeInTransaction(String db, java.util.function.Supplier<T> operations) throws DBStoreException {
		MongoDatabase mongoDb = getDB(db);
		// Get client from the database - this is a workaround since getClient() is not available
		MongoClient client = getMongoClient();
		ClientSession session = client.startSession();
		
		try {
			return session.withTransaction(new TransactionBody<T>() {
				@Override
				public T execute() {
					try {
						return operations.get();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		} catch (Exception e) {
			log.error("Transaction failed", e);
			throw new DBStoreException("Transaction failed", e);
		} finally {
			session.close();
		}
	}
	
	@Override
	public void executeInTransaction(String db, Runnable operations) throws DBStoreException {
		executeInTransaction(db, () -> {
			operations.run();
			return null;
		});
	}
	
	@Override
	public <T> T executeInTransaction(String db, java.util.function.Function<DBStoreTransactionContext, T> operations) throws DBStoreException {
		MongoDatabase mongoDb = getDB(db);
		// Get client from the database - this is a workaround since getClient() is not available
		MongoClient client = getMongoClient();
		ClientSession session = client.startSession();
		
		try {
			return session.withTransaction(new TransactionBody<T>() {
				@Override
				public T execute() {
					try {
						MongoTransactionContext context = new MongoTransactionContext(AMongoDataStore.this, db, session);
						return operations.apply(context);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		} catch (Exception e) {
			log.error("Transaction failed", e);
			throw new DBStoreException("Transaction failed", e);
		} finally {
			session.close();
		}
	}
	
	@Override
	public void executeInTransaction(String db, java.util.function.Consumer<DBStoreTransactionContext> operations) throws DBStoreException {
		executeInTransaction(db, context -> {
			operations.accept(context);
			return null;
		});
	}
	
	// Transaction-aware methods for internal use
	
	protected <T extends DBStoreEntity> T saveObjectInTransaction(String db, T object, ClientSession session) {
		if (object == null) {
			return null;
		}

		MongoDatabase mongoDb = getDB(db);
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, object.getClass());

		List<DBStoreListener<?>> entityListeners = getListeners(object.getClass());

		for (DBStoreListener listener : entityListeners) {
			log.debug("firing 'beforeSave' for: " + object.getClass() + " / " + object.getId());
			listener.beforeSave(db, object);
		}

		try {
			// Use session for transaction-aware operation
			coll.insertOne(session, object);

			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'created' for: " + object.getClass() + " / " + object.getId() + " / " + listener.getClass());
				listener.created(db, object);
			}

			return object;

		} catch (Exception e) {
			log.error("Error saving object: " + object, e);
			throw new DBStoreException("Error saving object", e);
		}
	}
	
	protected <T extends DBStoreEntity> T getObjectInTransaction(String db, Class<T> clazz, String id, ClientSession session) {
		if (id == null) {
			return null;
		}

		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);
		return coll.findOne(Filters.eq("_id", id));
	}
	
	protected <T extends DBStoreEntity> boolean deleteObjectInTransaction(String db, Class<T> clazz, String id, ClientSession session) {
		if (id == null) {
			return false;
		}

		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);
		DeleteResult result = coll.deleteOne(Filters.eq("_id", id));
		return result.getDeletedCount() > 0;
	}
	
	protected <T extends DBStoreEntity> boolean deleteObjectInTransaction(String db, T object, ClientSession session) {
		if (object == null || object.getId() == null) {
			return false;
		}
		return deleteObjectInTransaction(db, (Class<T>) object.getClass(), object.getId(), session);
	}
	
	protected <T extends DBStoreEntity> boolean deleteObjectsInTransaction(String db, Class<T> type, DBStoreQuery query, ClientSession session) {
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, type);
		Bson filter = fqtl.translate(query);
		DeleteResult result = coll.deleteMany(filter);
		return result.getDeletedCount() > 0;
	}
	
	protected <T extends DBStoreEntity> List<T> findObjectsInTransaction(String db, Class<T> clazz, DBStoreQuery query, ClientSession session) {
		FindIterable<T> f = getCollection(db, clazz)
				.find(fqtl.translate(query))
				.sort(fqtl.translateOrderBy(query));

		if (query != null) {
			int skip = query.getStart();
			if (skip > 0) {
				f = f.skip(skip);
			}

			int max = query.getMax();
			if (max > 0 && max < Integer.MAX_VALUE) {
				f = f.limit(max);
			}
		}

		List<T> out = new LinkedList<>();
		f.forEach(out::add);
		return out;
	}
	
	protected <T extends DBStoreEntity> T findObjectInTransaction(String db, Class<T> clazz, DBStoreQuery query, ClientSession session) {
		FindIterable<T> f = getCollection(db, clazz)
				.find(fqtl.translate(query))
				.sort(fqtl.translateOrderBy(query))
				.limit(1);

		return f.first();
	}
	
	protected <T extends DBStoreEntity> long countObjectsInTransaction(String db, Class<T> clazz, DBStoreQuery query, ClientSession session) {
		Bson filter = fqtl.translate(query);
		return getCollection(db, clazz).countDocuments(filter);
	}
	
	protected <T extends DBStoreEntity> List<T> saveObjectsInTransaction(String db, List<T> objects, ClientSession session) {
		if (objects == null || objects.isEmpty()) {
			return objects;
		}

		List<T> out = new LinkedList<>();
		for (T object : objects) {
			T saved = saveObjectInTransaction(db, object, session);
			out.add(saved);
		}
		return out;
	}
	
	protected <T extends DBStoreEntity> T updateObjectFieldsInTransaction(String db, Class<T> clazz, String id, Map<String, Object> fields, ClientSession session) {
		if (id == null || fields == null || fields.isEmpty()) {
			return null;
		}

		List<DBStoreListener<?>> entityListeners = getListeners(clazz);
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);

		T old = coll.findOne(Filters.eq("_id", id));
		if (old == null) {
			log.debug("No object found with id: " + id + " in class: " + clazz.getSimpleName());
			return null;
		}

		for (DBStoreListener listener : entityListeners) {
			log.debug("firing 'beforeSave' for: " + clazz + " / " + id);
			listener.beforeSave(db, old);
		}

		try {
			Bson updateDoc = Updates.combine(
					fields.entrySet().stream()
							.map(entry -> Updates.set(entry.getKey(), entry.getValue()))
							.collect(java.util.stream.Collectors.toList())
			);
			UpdateOptions options = new UpdateOptions();
			
			coll.updateOne(Filters.eq("_id", id), updateDoc, options);

			T updated = getObjectInTransaction(db, clazz, id, session);

			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'updated' for: " + clazz + " / " + id + " / " + listener.getClass());
				listener.updated(db, old, updated);
			}

			return updated;

		} catch (Exception e) {
			log.error("Error updating fields for object with id: " + id, e);
			throw new DBStoreException("Error updating fields for object with id: " + id, e);
		}
	}
	
	protected <T extends DBStoreEntity> T updateObjectFieldsInTransaction(String db, Class<T> clazz, String id, List<FieldUpdate> fieldUpdates, ClientSession session) {
		if (id == null || fieldUpdates == null || fieldUpdates.isEmpty()) {
			return null;
		}

		List<DBStoreListener<?>> entityListeners = getListeners(clazz);
		JacksonMongoCollection<T> coll = (JacksonMongoCollection<T>) getCollection(db, clazz);

		T old = coll.findOne(Filters.eq("_id", id));
		if (old == null) {
			log.debug("No object found with id: " + id + " in class: " + clazz.getSimpleName());
			return null;
		}

		for (DBStoreListener listener : entityListeners) {
			log.debug("firing 'beforeSave' for: " + clazz + " / " + id);
			listener.beforeSave(db, old);
		}

		try {
			List<Bson> updateOperations = new ArrayList<>();
			for (FieldUpdate fieldUpdate : fieldUpdates) {
				Bson operation = createUpdateOperation(fieldUpdate);
				if (operation != null) {
					updateOperations.add(operation);
				}
			}

			if (updateOperations.isEmpty()) {
				log.debug("No valid update operations found");
				return old;
			}

			Bson updateDoc = Updates.combine(updateOperations);
			UpdateOptions options = new UpdateOptions();
			
			coll.updateOne(Filters.eq("_id", id), updateDoc, options);

			T updated = getObjectInTransaction(db, clazz, id, session);

			for (DBStoreListener listener : entityListeners) {
				log.debug("firing 'updated' for: " + clazz + " / " + id + " / " + listener.getClass());
				listener.updated(db, old, updated);
			}

			return updated;

		} catch (Exception e) {
			log.error("Error updating fields for object with id: " + id, e);
			throw new DBStoreException("Error updating fields for object with id: " + id, e);
		}
	}
	
	protected <T extends DBStoreEntity> T incrementFieldInTransaction(String db, Class<T> clazz, String id, String fieldName, Number increment, ClientSession session) {
		return updateObjectFieldsInTransaction(db, clazz, id, Collections.singletonList(FieldUpdate.inc(fieldName, increment)), session);
	}
	
	protected <T extends DBStoreEntity> T setFieldInTransaction(String db, Class<T> clazz, String id, String fieldName, Object value, ClientSession session) {
		return updateObjectFieldsInTransaction(db, clazz, id, Collections.singletonList(FieldUpdate.set(fieldName, value)), session);
	}
	
	protected <T extends DBStoreEntity> T unsetFieldInTransaction(String db, Class<T> clazz, String id, String fieldName, ClientSession session) {
		return updateObjectFieldsInTransaction(db, clazz, id, Collections.singletonList(FieldUpdate.unset(fieldName)), session);
	}
	
	protected void saveBinaryInTransaction(String db, String bucket, DBStoreBinary binary, ClientSession session) throws DBStoreException {
		if (binary == null || binary.getId() == null) {
			throw new DBStoreException("Binary or binary ID cannot be null");
		}

		GridFSBucket gridFSBucket = getBucket(db, bucket);
		GridFSUploadOptions options = new GridFSUploadOptions();
		Map<String, Object> metadata = binary.getMetaData();
		if (metadata != null && metadata.containsKey("contentType")) {
			options.metadata(new Document("contentType", metadata.get("contentType")));
		}

		try {
			gridFSBucket.uploadFromStream(binary.getId(), binary.getInputStream(), options);
		} catch (Exception e) {
			log.error("Error saving binary: " + binary.getId(), e);
			throw new DBStoreException("Error saving binary: " + binary.getId(), e);
		}
	}
	
	protected DBStoreBinary getBinaryInTransaction(String db, String bucket, String id, ClientSession session) throws DBStoreException {
		if (id == null) {
			return null;
		}

		GridFSBucket gridFSBucket = getBucket(db, bucket);
		
		try {
			GridFSFile file = gridFSBucket.find(Filters.eq("_id", id)).first();
			if (file == null) {
				return null;
			}

			// Create input stream from GridFS
			GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(id);
			
			Map<String, Object> metadata = new HashMap<>();
			Document fileMetadata = file.getMetadata();
			if (fileMetadata != null && fileMetadata.containsKey("contentType")) {
				metadata.put("contentType", fileMetadata.getString("contentType"));
			}

			return new BasicBinary(id, downloadStream, file.getLength(), metadata);

		} catch (Exception e) {
			log.error("Error getting binary: " + id, e);
			throw new DBStoreException("Error getting binary: " + id, e);
		}
	}
	
}
