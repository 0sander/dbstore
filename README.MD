**DBSTORE**
=================

this is a simple abstraction / utility layer on top of mongo db.  from version 0.10, these are the minimum requirements:

- Spring Framework 6.1 (Spring Boot 3.0+)
- MongoDB Driver 5.0
- Mongojack 5.0.3
- MongoDB 4.4+ (with replica set for transaction support)

from version 0.10.13+ added full transaction support and atomic field update operations.

it contains a few basic classes that might help, specifically:

***DbQuery***

this is an interface for a builder style class to create queries. a basic implementation called "BasicDBQuery" is provided. simple usage:

	DbQuery q = BasicQuery.createQuery();
	q = q.eq("fieldname","value");
	q = q.gte("fieldname2","value2");
	[...]


​
***DataStore***

an interface for a data store that can be queried with a DbQuery. an implementation for mongo (along with the necessary translator to translate DbQuery into an actual mongo query is provided in dbstore-mongo and dbstore-query-mongo). simple usage:


	DbQuery q = BasicQuery.createQuery();
	q = q.eq("fieldname","value");
	q = q.gte("fieldname2","value2");
	[...]
	DataStore ds = getDataStore();
	return ds.findObjects(MyClass.class,q);

and, for saving:

	ds.saveObject("my_db",object);

note that your objects have to extend `DBStoreEntity`.

***Transaction Support***

from version 0.10.13+, the DataStore supports MongoDB transactions. transactions require a MongoDB replica set or sharded cluster:

```java
// Execute operations in a transaction
String result = dataStore.executeInTransaction("database", () -> {
    dataStore.saveObject("database", entity1);
    dataStore.saveObject("database", entity2);
    return "success";
});

// With transaction context for session-aware operations
dataStore.executeInTransaction("database", (context) -> {
    context.saveObject(entity1);
    context.updateObjectFields(Entity.class, id, updates);
    return result;
});

// Check if transactions are supported
if (dataStore.supportsTransactions()) {
    // Use transaction operations
}
```

***Atomic Field Updates***

the DataStore now supports atomic field update operations using MongoDB's atomic operators:

```java
// Increment a field atomically
dataStore.incrementField("database", Entity.class, id, "counter", 5);

// Set a field
dataStore.setField("database", Entity.class, id, "status", "active");

// Unset a field
dataStore.unsetField("database", Entity.class, id, "tempField");

// Complex updates with multiple operations
List<FieldUpdate> updates = Arrays.asList(
    FieldUpdate.inc("counter", 1),
    FieldUpdate.set("status", "updated"),
    FieldUpdate.push("tags", "new-tag"),
    FieldUpdate.addToSet("categories", "premium")
);
dataStore.updateObjectFields("database", Entity.class, id, updates);
```

available atomic operations:
- `FieldUpdate.set(field, value)` - Set a field value
- `FieldUpdate.inc(field, number)` - Increment a numeric field
- `FieldUpdate.unset(field)` - Remove a field
- `FieldUpdate.push(field, value)` - Add value to array
- `FieldUpdate.pull(field, value)` - Remove value from array
- `FieldUpdate.addToSet(field, value)` - Add unique value to array
- `FieldUpdate.mul(field, number)` - Multiply numeric field
- `FieldUpdate.min(field, value)` - Set minimum value
- `FieldUpdate.max(field, value)` - Set maximum value
- `FieldUpdate.rename(field, newName)` - Rename a field
- `FieldUpdate.setOnInsert(field, value)` - Set value only on insert

***Spring Transaction Integration***

the DataStore integrates with Spring's transaction management:

```java
@Transactional
public void businessMethod() {
    // All operations automatically participate in transaction
    dataStore.saveObject("database", entity);
    dataStore.updateObjectFields("database", Entity.class, id, updates);
}
```

configuration:
```java
@Configuration
@EnableTransactionManagement
public class DBStoreConfig {
    @Bean
    public PlatformTransactionManager transactionManager(DataStore dataStore) {
        return new DBStoreTransactionManager(dataStore, "defaultDatabase");
    }
}
```

***DbStoreListener***

this is an interface for anything that's interested in receiving notifications on the persistence of specific objects. in a spring context (if your data store is a bean in your spring context), these will be autowired. a basic adaptor called `"DBStoreListenerAdapter"` is provided.

#### Annotations

this package contains a few annotations that can come in handy, most importantly:

``` asd
@Indexes({
		@Index(name = "firstNameIdx", fields = "firstName"),
		@Index(name = "lastNameIdx", fields = "lastName"),
		@Index(name = "fullNameIdx", fields = {"firstName", "lastName"}, unique = true)
})
public class EntityWithIndexes extends BaseDBStoreEntity {
		...
}
```

for creating indexes on collections of entities.

also:

```
@CollectionName(CollectionName.USE_CLASS_NAME)
public class Entity extends BaseDBStoreEntity {
		...
}
```

for configuring how collections are named.



***How To Use This In A Maven Project***

to use this in a maven project, add our repo to your classpath:

	<repositories>
		<repository>
			<id>dbstore-releases</id>
			<url>https://raw.githubusercontent.com/rmalchow/maven/master/</url>
		</repository>
	</repositories>

and then depend on the API artifacts:

	<dependency>
		<groupId>com.cinefms.dbstore</groupId>
		<artifactId>dbstore-api</artifactId>
		<version>${dbstore.version}</version>
	</dependency>
	<dependency>
		<groupId>com.cinefms.dbstore</groupId>
		<artifactId>dbstore-query-api</artifactId>
		<version>${dbstore.version}</version>
	</dependency>

and possibly the implementations:

	<dependency>
		<groupId>com.cinefms.dbstore</groupId>
		<artifactId>dbstore-mongo</artifactId>
		<version>${dbstore.version}</version>
	</dependency>

this is meant to be used with spring, and it contains a couple of classes to do autoconfig in a spring context, so that (apart from some env variables) everything should be wired up correctly out of the box.

questions? ask me! i'll try to help.
