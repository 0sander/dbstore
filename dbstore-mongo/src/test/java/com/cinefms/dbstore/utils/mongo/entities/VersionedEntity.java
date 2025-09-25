package com.cinefms.dbstore.utils.mongo.entities;

import com.cinefms.dbstore.api.impl.BaseDBStoreEntity;

import java.beans.ConstructorProperties;

public class VersionedEntity extends BaseDBStoreEntity {

	public String name;
	public int version;
	public int counter;

	@ConstructorProperties({"name", "version", "counter"})
	public VersionedEntity(String name, int version, int counter) {
		this.name = name;
		this.version = version;
		this.counter = counter;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}
}
