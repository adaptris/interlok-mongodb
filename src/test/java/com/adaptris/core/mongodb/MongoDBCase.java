/*
 * Copyright 2018 Adaptris Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adaptris.core.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.util.TimeInterval;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * @author mwarman
 */
public abstract class MongoDBCase extends ExampleProducerCase {

  MongoDBConnection connection;
  MongoDatabase database;
  MongoCollection<Document> collection;

  final boolean localTests;
  String connectionUri;

  static final String COLLECTION = "collection";

  static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  public MongoDBCase() {
    localTests = PROPERTIES.getProperty("local.tests") != null && PROPERTIES.getProperty("local.tests").equals("true");
    if (PROPERTIES.getProperty("local.test.connection.uri") != null) {
      connectionUri = PROPERTIES.getProperty("local.test.connection.uri");
    }
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    if (localTests) {
      connection = new MongoDBConnection(connectionUri, "database");
      LifecycleHelper.initAndStart(connection);
      database = connection.retrieveMongoDatabase();
      collection = database.getCollection(COLLECTION);
    } else {
      connection = spy(new MongoDBConnection());
      MongoClient mongoClient = mock(MongoClient.class);
      database = mock(MongoDatabase.class);
      doReturn(mongoClient).when(connection).retrieveMongoClient();
      doReturn(database).when(connection).retrieveMongoDatabase();
      collection = mock(MongoCollection.class);
      doReturn(collection).when(database).getCollection(COLLECTION);
      UpdateResult updateResult = mock(UpdateResult.class);
      doReturn(updateResult).when(collection).updateOne(any(Bson.class), any(Bson.class));
      doReturn(updateResult).when(collection).updateOne(any(Bson.class), any(Bson.class), any(UpdateOptions.class));
      doReturn(updateResult).when(collection).replaceOne(any(Bson.class), any(), any(ReplaceOptions.class));
      DeleteResult deleteResult = mock(DeleteResult.class);
      doReturn(deleteResult).when(collection).deleteOne(any());
    }
    onSetup();
  }

  protected void onSetup() throws Exception {
    // For sub-class to override
  }

  @AfterEach
  public void tearDown() {
    if (localTests) {
      clearData();
      LifecycleHelper.stopAndClose(connection);
    }
  }

  void assertJsonArraySize(String contents, int size) throws JSONException {
    final JSONArray array = new JSONArray(contents);
    assertEquals(size, array.length());
  }

  void clearData() {
    if (localTests) {
      collection.deleteMany(Document.parse("{}"));
    }
  }

}