/*
 * Copyright 2011 VZ Netzwerke Ltd
 * Copyright 2014 devbliss GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mongojack.internal;

import com.fasterxml.jackson.databind.SerializerProvider;
import org.mongojack.DBRef;
import org.mongojack.internal.object.BsonObjectGenerator;
import org.mongojack.internal.stream.DBEncoderBsonGenerator;

import java.io.IOException;

/**
 * Serialises DBRef objects
 *
 * @author James Roper
 * @since 1.2
 */
public class DBRefSerializer extends MongoSerializer<DBRef> {

	@Override
	public void serialize(DBRef value, DBEncoderBsonGenerator bgen,
						  SerializerProvider provider) throws IOException {
		if (value == null) {
			bgen.writeNull();
		} else {
			bgen.writeStartObject();
			bgen.writeFieldName("$ref");
			bgen.writeString(value.getCollectionName());
			bgen.writeFieldName("$id");
			bgen.writeObject(value.getId());
			bgen.writeEndObject();
		}
	}

	@Override
	protected void serialize(DBRef value, BsonObjectGenerator bgen,
							 SerializerProvider provider) {
		if (value == null) {
			bgen.writeNull();
		} else {
			bgen.writeObject(new com.mongodb.DBRef(value.getCollectionName(), value.getId()));
		}
	}

	@Override
	public Class<DBRef> handledType() {
		return DBRef.class;
	}
}
