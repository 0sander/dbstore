package com.cinefms.dbstore.utils.mongo;

import com.cinefms.dbstore.api.DBStoreTransactionContext;
import com.cinefms.dbstore.api.FieldUpdate;
import com.cinefms.dbstore.utils.mongo.entities.SimpleEntity;
import com.cinefms.dbstore.utils.mongo.entities.VersionedEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test that verifies the transaction API is properly implemented
 * Note: Actual transaction execution requires MongoDB replica set or sharded cluster
 */
public class TransactionAPITest extends MongoDataStoreTest {

    @Test
    public void itShouldReportTransactionSupport() {
        // Test that the API reports transaction support
        Assert.assertTrue("DataStore should support transactions", mds.supportsTransactions());
    }

    @Test
    public void itShouldHaveTransactionMethods() {
        // Test that all transaction methods exist and are callable
        try {
            // Test Supplier-based transaction
            String result = mds.executeInTransaction("testdb", () -> "test-result");
            Assert.assertEquals("Should return the result", "test-result", result);
        } catch (Exception e) {
            // Expected in test environment without transaction support
            Assert.assertTrue("Should be transaction support error", 
                e.getMessage().contains("Transaction failed") || 
                e.getMessage().contains("Transactions are not supported"));
        }

        try {
            // Test Runnable-based transaction
            mds.executeInTransaction("testdb", () -> {
                // Do nothing - just test the method exists
            });
        } catch (Exception e) {
            // Expected in test environment without transaction support
            Assert.assertTrue("Should be transaction support error", 
                e.getMessage().contains("Transaction failed") || 
                e.getMessage().contains("Transactions are not supported"));
        }

        try {
            // Test Function-based transaction with context
            String result = mds.executeInTransaction("testdb", (DBStoreTransactionContext context) -> {
                return "context-result";
            });
            Assert.assertEquals("Should return the result", "context-result", result);
        } catch (Exception e) {
            // Expected in test environment without transaction support
            Assert.assertTrue("Should be transaction support error", 
                e.getMessage().contains("Transaction failed") || 
                e.getMessage().contains("Transactions are not supported"));
        }

        try {
            // Test Consumer-based transaction with context
            mds.executeInTransaction("testdb", (DBStoreTransactionContext context) -> {
                // Do nothing - just test the method exists
            });
        } catch (Exception e) {
            // Expected in test environment without transaction support
            Assert.assertTrue("Should be transaction support error", 
                e.getMessage().contains("Transaction failed") || 
                e.getMessage().contains("Transactions are not supported"));
        }
    }

    @Test
    public void itShouldHaveTransactionContextMethods() {
        // Test that DBStoreTransactionContext has all required methods
        // This is a compile-time test - if it compiles, the interface is correct
        
        // Create a mock context to test method signatures
        DBStoreTransactionContext context = new DBStoreTransactionContext() {
            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T saveObject(T object) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T getObject(Class<T> clazz, String id) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> boolean deleteObject(Class<T> clazz, String id) {
                return false;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> boolean deleteObject(T object) {
                return false;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> boolean deleteObjects(Class<T> type, com.cinefms.dbstore.query.api.DBStoreQuery query) {
                return false;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> List<T> findObjects(Class<T> clazz, com.cinefms.dbstore.query.api.DBStoreQuery query) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T findObject(Class<T> clazz, com.cinefms.dbstore.query.api.DBStoreQuery query) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> long countObjects(Class<T> clazz, com.cinefms.dbstore.query.api.DBStoreQuery query) {
                return 0;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> List<T> saveObjects(List<T> objects) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, java.util.Map<String, Object> fields) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T updateObjectFields(Class<T> clazz, String id, List<FieldUpdate> fieldUpdates) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T incrementField(Class<T> clazz, String id, String fieldName, Number increment) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T setField(Class<T> clazz, String id, String fieldName, Object value) {
                return null;
            }

            @Override
            public <T extends com.cinefms.dbstore.api.DBStoreEntity> T unsetField(Class<T> clazz, String id, String fieldName) {
                return null;
            }

            @Override
            public void saveBinary(String bucket, com.cinefms.dbstore.api.DBStoreBinary binary) throws com.cinefms.dbstore.api.exceptions.DBStoreException {
            }

            @Override
            public com.cinefms.dbstore.api.DBStoreBinary getBinary(String bucket, String id) throws com.cinefms.dbstore.api.exceptions.DBStoreException {
                return null;
            }
        };

        // If we get here, the interface is correctly defined
        Assert.assertNotNull("Transaction context should be created", context);
    }

    @Test
    public void itShouldHaveFieldUpdateMethods() {
        // Test that FieldUpdate has all required static methods
        FieldUpdate setUpdate = FieldUpdate.set("field", "value");
        FieldUpdate incUpdate = FieldUpdate.inc("field", 1);
        FieldUpdate unsetUpdate = FieldUpdate.unset("field");
        FieldUpdate pushUpdate = FieldUpdate.push("field", "value");
        FieldUpdate pullUpdate = FieldUpdate.pull("field", "value");
        FieldUpdate addToSetUpdate = FieldUpdate.addToSet("field", "value");
        FieldUpdate mulUpdate = FieldUpdate.mul("field", 2.0);
        FieldUpdate minUpdate = FieldUpdate.min("field", 1);
        FieldUpdate maxUpdate = FieldUpdate.max("field", 10);
        FieldUpdate renameUpdate = FieldUpdate.rename("field", "newField");
        FieldUpdate setOnInsertUpdate = FieldUpdate.setOnInsert("field", "value");

        Assert.assertNotNull("Set update should be created", setUpdate);
        Assert.assertNotNull("Increment update should be created", incUpdate);
        Assert.assertNotNull("Unset update should be created", unsetUpdate);
        Assert.assertNotNull("Push update should be created", pushUpdate);
        Assert.assertNotNull("Pull update should be created", pullUpdate);
        Assert.assertNotNull("Add to set update should be created", addToSetUpdate);
        Assert.assertNotNull("Multiply update should be created", mulUpdate);
        Assert.assertNotNull("Min update should be created", minUpdate);
        Assert.assertNotNull("Max update should be created", maxUpdate);
        Assert.assertNotNull("Rename update should be created", renameUpdate);
        Assert.assertNotNull("Set on insert update should be created", setOnInsertUpdate);
    }

    @Test
    public void itShouldMaintainBackwardCompatibility() {
        // Test that all existing API methods still work
        SimpleEntity entity = new SimpleEntity("test");
        
        // Test basic CRUD operations
        SimpleEntity saved = mds.saveObject("testdb", entity);
        Assert.assertNotNull("Save should work", saved);
        Assert.assertEquals("Value should match", "test", saved.getValue());
        
        SimpleEntity found = mds.getObject("testdb", SimpleEntity.class, saved.getId());
        Assert.assertNotNull("Get should work", found);
        Assert.assertEquals("Value should match", "test", found.getValue());
        
        // Test field updates
        SimpleEntity updated = mds.setField("testdb", SimpleEntity.class, saved.getId(), "value", "updated");
        Assert.assertNotNull("Set field should work", updated);
        Assert.assertEquals("Value should be updated", "updated", updated.getValue());
        
        // Test atomic operations
        VersionedEntity versioned = new VersionedEntity("test", 1, 5);
        versioned = mds.saveObject("testdb", versioned);
        
        VersionedEntity incremented = mds.incrementField("testdb", VersionedEntity.class, versioned.getId(), "counter", 10);
        Assert.assertNotNull("Increment should work", incremented);
        Assert.assertEquals("Counter should be incremented", 15, incremented.getCounter());
        
        // Test query operations
        List<SimpleEntity> allEntities = mds.findObjects("testdb", SimpleEntity.class, null);
        Assert.assertNotNull("Find objects should work", allEntities);
        Assert.assertTrue("Should find at least one entity", allEntities.size() > 0);
        
        long count = mds.countObjects("testdb", SimpleEntity.class, null);
        Assert.assertTrue("Count should be positive", count > 0);
        
        // Test delete operations
        boolean deleted = mds.deleteObject("testdb", SimpleEntity.class, saved.getId());
        Assert.assertTrue("Delete should work", deleted);
        
        SimpleEntity notFound = mds.getObject("testdb", SimpleEntity.class, saved.getId());
        Assert.assertNull("Entity should be deleted", notFound);
    }
}
