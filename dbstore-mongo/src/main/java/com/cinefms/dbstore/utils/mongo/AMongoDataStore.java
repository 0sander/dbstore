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
import com.cinefms.dbstore.api.DBStoreEntity;
import com.cinefms.dbstore.api.DBStoreListener;
import com.cinefms.dbstore.api.DataStore;
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
	
}
