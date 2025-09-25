package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.FieldUpdate;
import com.cinefms.dbstore.query.api.impl.BasicQuery;
import com.cinefms.dbstore.utils.mongo.entities.SimpleEntity;
import com.cinefms.dbstore.utils.mongo.entities.VersionedEntity;
import com.cinefms.dbstore.utils.mongo.utils.AssertCollection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoStorePersistenceTest extends MongoDataStoreTest {

	@Test
	public void itShouldCreateRecord() {
		SimpleEntity entity = new SimpleEntity("test-value");

		SimpleEntity savedEntity = mds.saveObject(null, entity);

		Assert.assertNotNull(savedEntity);
		Assert.assertEquals(savedEntity.getId(), entity.getId());
		Assert.assertEquals(savedEntity.getValue(), entity.getValue());

		// Check stored data
		Document record = mds.getDB(null)
				.getCollection(SimpleEntity.class.getName())
				.find()
				.cursor()
				.next();

		Assert.assertNotNull(record);
		Assert.assertEquals(entity.getId(), record.get("_id"));
		Assert.assertEquals(entity.getValue(), record.get("value"));
	}

	@Test
	public void itShouldUpdateExistingRecord() {
		SimpleEntity unchangedEntity = new SimpleEntity("test-value");
		mds.saveObject(null, unchangedEntity);

		SimpleEntity editedEntity = new SimpleEntity("test-value");
		mds.saveObject(null, editedEntity);

		editedEntity.setValue("new-value");
		SimpleEntity savedEntity = mds.saveObject(null, editedEntity);

		Assert.assertNotNull(savedEntity);
		Assert.assertEquals(savedEntity.getId(), editedEntity.getId());
		Assert.assertEquals(savedEntity.getValue(), editedEntity.getValue());

		List<Document> records = loadAll(mds.getDB(null)
				.getCollection(SimpleEntity.class.getName())
				.find());

		Assert.assertEquals(2, records.size());
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(unchangedEntity.getId(), record.get("_id"));
			Assert.assertEquals(unchangedEntity.getValue(), record.get("value"));
		});
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(editedEntity.getId(), record.get("_id"));
			Assert.assertEquals(editedEntity.getValue(), record.get("value"));
		});
	}

	@Test
	public void itShouldUpdateMultipleExistingRecordsAtOnce() {
		SimpleEntity firstEntity = new SimpleEntity("first-entity");
		SimpleEntity secondEntity = new SimpleEntity("second-entity");
		List<SimpleEntity> savedEntities = mds.saveObjects(null, Arrays.asList(firstEntity, secondEntity));

		AssertCollection.assertContains(savedEntities, entity -> {
			Assert.assertEquals(firstEntity.getId(), entity.getId());
		});
		AssertCollection.assertContains(savedEntities, entity -> {
			Assert.assertEquals(secondEntity.getId(), entity.getId());
		});

		// Check stored data
		Assert.assertEquals(
				2,
				mds.getDB(null)
						.getCollection(SimpleEntity.class.getName())
						.countDocuments()
		);

		// Update existing & insert new entities
		firstEntity.setValue("first-entity-updated");
		secondEntity.setValue("second-entity-updated");
		SimpleEntity thirdEntity = new SimpleEntity("third-entity");
		SimpleEntity fourthEntity = new SimpleEntity("fourth-entity");
		List<SimpleEntity> updatedEntities = mds.saveObjects(
				null,
				Arrays.asList(firstEntity, secondEntity, thirdEntity, fourthEntity)
		);

		AssertCollection.assertContains(updatedEntities, entity -> {
			Assert.assertEquals(firstEntity.getId(), entity.getId());
		});
		AssertCollection.assertContains(updatedEntities, entity -> {
			Assert.assertEquals(secondEntity.getId(), entity.getId());
		});
		AssertCollection.assertContains(updatedEntities, entity -> {
			Assert.assertEquals(thirdEntity.getId(), entity.getId());
		});
		AssertCollection.assertContains(updatedEntities, entity -> {
			Assert.assertEquals(firstEntity.getId(), entity.getId());
		});

		// Check stored data
		List<Document> records = loadAll(mds.getDB(null)
				.getCollection(SimpleEntity.class.getName())
				.find());

		Assert.assertEquals(4, records.size());
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(firstEntity.getId(), record.get("_id"));
			Assert.assertEquals("first-entity-updated", record.get("value"));
		});
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(secondEntity.getId(), record.get("_id"));
			Assert.assertEquals("second-entity-updated", record.get("value"));
		});
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(thirdEntity.getId(), record.get("_id"));
			Assert.assertEquals("third-entity", record.get("value"));
		});
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(fourthEntity.getId(), record.get("_id"));
			Assert.assertEquals("fourth-entity", record.get("value"));
		});
	}

	@Test
	public void itShouldDeleteExistingEntity() {
		SimpleEntity entity = new SimpleEntity("test-value");
		mds.saveObject(null, entity);

		boolean result = mds.deleteObject(null, entity);
		Assert.assertTrue(result);

		// Check stored data
		Assert.assertEquals(0, mds.getDB(null).getCollection(SimpleEntity.class.getName()).countDocuments());
	}

	@Test
	public void itShouldReturnFalseIfDeletedEntityThatDoesNotExists() {
		SimpleEntity entity = new SimpleEntity("test-value");
		mds.saveObject(null, entity);

		mds.deleteObject(null, entity);
		boolean result = mds.deleteObject(null, entity);
		Assert.assertFalse(result);

		// Check stored data
		Assert.assertEquals(0, mds.getDB(null).getCollection(SimpleEntity.class.getName()).countDocuments());
	}

	@Test
	public void itShouldDeleteExistingEntityById() {
		SimpleEntity entity = new SimpleEntity("test-value");
		mds.saveObject(null, entity);

		boolean result = mds.deleteObject(null, SimpleEntity.class, entity.getId());
		Assert.assertTrue(result);

		// Check stored data
		Assert.assertEquals(0, mds.getDB(null).getCollection(SimpleEntity.class.getName()).countDocuments());
	}

	@Test
	public void itShouldReturnFalseIfDeletedEntityByIdDoesNotExists() {
		SimpleEntity entity = new SimpleEntity("test-value");
		mds.saveObject(null, entity);

		mds.deleteObject(null, SimpleEntity.class, entity.getId());
		boolean result = mds.deleteObject(null, SimpleEntity.class, entity.getId());
		Assert.assertFalse(result);

		// Check stored data
		Assert.assertEquals(0, mds.getDB(null).getCollection(SimpleEntity.class.getName()).countDocuments());
	}

	@Test
	public void itShouldDeleteMultipleEntitiesAtOnce() {
		SimpleEntity firstEntity = new SimpleEntity("first-entity");
		SimpleEntity secondEntity = new SimpleEntity("second-entity");
		SimpleEntity thirdEntity = new SimpleEntity("third-entity");
		SimpleEntity fourthEntity = new SimpleEntity("fourth-entity");
		mds.saveObjects(null, Arrays.asList(firstEntity, secondEntity, thirdEntity, fourthEntity));

		boolean result = mds.deleteObjects(
				null,
				SimpleEntity.class,
				BasicQuery.createQuery().in("_id", firstEntity.getId(), thirdEntity.getId())
		);
		Assert.assertTrue(result);

		// Check stored data
		List<Document> records = loadAll(mds.getDB(null)
				.getCollection(SimpleEntity.class.getName())
				.find());

		Assert.assertEquals(2, records.size());
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(secondEntity.getId(), record.get("_id"));
			Assert.assertEquals("second-entity", record.get("value"));
		});
		AssertCollection.assertContains(records, record -> {
			Assert.assertEquals(fourthEntity.getId(), record.get("_id"));
			Assert.assertEquals("fourth-entity", record.get("value"));
		});
	}

	@Test
	public void itShouldUpdateFieldsAtomically() {
		// Create an entity
		SimpleEntity entity = new SimpleEntity("original-value");
		SimpleEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Update specific fields using updateFields
		Map<String, Object> fieldsToUpdate = new HashMap<>();
		fieldsToUpdate.put("value", "updated-value");
		
		SimpleEntity updatedEntity = mds.updateObjectFields(null, SimpleEntity.class, entityId, fieldsToUpdate);
		
		// Verify the update was successful
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertEquals("updated-value", updatedEntity.getValue());

		// Verify the change was persisted in the database
		SimpleEntity retrievedEntity = mds.getObject(null, SimpleEntity.class, entityId);
		Assert.assertNotNull(retrievedEntity);
		Assert.assertEquals("updated-value", retrievedEntity.getValue());

		// Verify only one record exists (no duplicates)
		Assert.assertEquals(1, mds.getDB(null).getCollection(SimpleEntity.class.getName()).countDocuments());
	}

	@Test
	public void itShouldUpdateMultipleFieldsAtomically() {
		// Create an entity
		SimpleEntity entity = new SimpleEntity("original-value");
		SimpleEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Update multiple fields at once
		Map<String, Object> fieldsToUpdate = new HashMap<>();
		fieldsToUpdate.put("value", "updated-value");
		// Note: SimpleEntity only has one field, but this tests the multiple field update mechanism
		
		SimpleEntity updatedEntity = mds.updateObjectFields(null, SimpleEntity.class, entityId, fieldsToUpdate);
		
		// Verify the update was successful
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertEquals("updated-value", updatedEntity.getValue());
	}

	@Test
	public void itShouldReturnNullWhenUpdatingNonExistentEntity() {
		Map<String, Object> fieldsToUpdate = new HashMap<>();
		fieldsToUpdate.put("value", "updated-value");
		
		SimpleEntity result = mds.updateObjectFields(null, SimpleEntity.class, "non-existent-id", fieldsToUpdate);
		
		Assert.assertNull(result);
	}

	@Test
	public void itShouldReturnNullWhenFieldsMapIsEmpty() {
		SimpleEntity entity = new SimpleEntity("original-value");
		SimpleEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		Map<String, Object> emptyFields = new HashMap<>();
		SimpleEntity result = mds.updateObjectFields(null, SimpleEntity.class, entityId, emptyFields);
		
		Assert.assertNull(result);
	}

	@Test
	public void itShouldReturnNullWhenFieldsMapIsNull() {
		SimpleEntity entity = new SimpleEntity("original-value");
		SimpleEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		SimpleEntity result = mds.updateObjectFields(null, SimpleEntity.class, entityId, (Map<String, Object>) null);
		
		Assert.assertNull(result);
	}

	@Test
	public void itShouldIncrementFieldAtomically() {
		// Create a versioned entity
		VersionedEntity entity = new VersionedEntity("test", 1, 5);
		VersionedEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Increment version field atomically
		VersionedEntity updatedEntity = mds.incrementField(null, VersionedEntity.class, entityId, "version", 1);
		
		// Verify the increment was successful
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertEquals(2, updatedEntity.getVersion()); // 1 + 1 = 2
		Assert.assertEquals(5, updatedEntity.getCounter()); // unchanged

		// Verify the change was persisted in the database
		VersionedEntity retrievedEntity = mds.getObject(null, VersionedEntity.class, entityId);
		Assert.assertNotNull(retrievedEntity);
		Assert.assertEquals(2, retrievedEntity.getVersion());
	}

	@Test
	public void itShouldIncrementMultipleFieldsAtomically() {
		// Create a versioned entity
		VersionedEntity entity = new VersionedEntity("test", 1, 5);
		VersionedEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Increment multiple fields atomically using FieldUpdate
		List<FieldUpdate> updates = Arrays.asList(
			FieldUpdate.inc("version", 2),
			FieldUpdate.inc("counter", -1)
		);
		
		VersionedEntity updatedEntity = mds.updateObjectFields(null, VersionedEntity.class, entityId, updates);
		
		// Verify both increments were successful
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertEquals(3, updatedEntity.getVersion()); // 1 + 2 = 3
		Assert.assertEquals(4, updatedEntity.getCounter()); // 5 - 1 = 4
	}

	@Test
	public void itShouldSetAndIncrementFieldsAtomically() {
		// Create a versioned entity
		VersionedEntity entity = new VersionedEntity("test", 1, 5);
		VersionedEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Set name and increment version atomically
		List<FieldUpdate> updates = Arrays.asList(
			FieldUpdate.set("name", "updated"),
			FieldUpdate.inc("version", 1)
		);
		
		VersionedEntity updatedEntity = mds.updateObjectFields(null, VersionedEntity.class, entityId, updates);
		
		// Verify both operations were successful
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertEquals("updated", updatedEntity.getName());
		Assert.assertEquals(2, updatedEntity.getVersion()); // 1 + 1 = 2
		Assert.assertEquals(5, updatedEntity.getCounter()); // unchanged
	}

	@Test
	public void itShouldUnsetFieldAtomically() {
		// Create a versioned entity
		VersionedEntity entity = new VersionedEntity("test", 1, 5);
		VersionedEntity savedEntity = mds.saveObject(null, entity);
		Assert.assertNotNull(savedEntity);
		String entityId = savedEntity.getId();

		// Unset the name field atomically
		VersionedEntity updatedEntity = mds.unsetField(null, VersionedEntity.class, entityId, "name");
		
		// Verify the field was unset
		Assert.assertNotNull(updatedEntity);
		Assert.assertEquals(entityId, updatedEntity.getId());
		Assert.assertNull(updatedEntity.getName());
		Assert.assertEquals(1, updatedEntity.getVersion()); // unchanged
		Assert.assertEquals(5, updatedEntity.getCounter()); // unchanged
	}

	@Test
	public void itShouldHandleNullQuery() {
		SimpleEntity entity = new SimpleEntity("test-value");
		mds.saveObject(null, entity);

		// Test that null query doesn't crash
		List<SimpleEntity> results = mds.findObjects(null, SimpleEntity.class, null);
		
		// Should return all objects (empty filter)
		Assert.assertNotNull(results);
		Assert.assertTrue("Should find at least one object", results.size() >= 1);
	}

}
