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

import com.adaptris.core.ProducerCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * @author mwarman
 */
public abstract class MongoDBCase extends ProducerCase {

  MongoDBConnection connection;
  MongoDatabase database;
  MongoCollection collection;

  final boolean localTests;
  String connectionUri;

  static final String COLLECTION = "collection";

  static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  public MongoDBCase(){
    localTests = PROPERTIES.getProperty("local.tests") != null && PROPERTIES.getProperty("local.tests").equals("true");
    if(PROPERTIES.getProperty("local.test.connection.uri") != null) {
      connectionUri = PROPERTIES.getProperty("local.test.connection.uri");
    }
  }

  @Before
  public void setUp() throws Exception{
    if(localTests){
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
    }
  }

  @After
  public void tearDown(){
    if (localTests){
      collection.deleteMany(Document.parse("{}"));
      LifecycleHelper.stopAndClose(connection);
    }
  }

  void assertJsonArraySize(String contents, int size) throws ParseException {
    final JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
    final Object object = jsonParser.parse(contents);
    if (object instanceof JSONObject) {
      fail();
    } else if (object instanceof JSONArray) {
      final JSONArray array = (JSONArray)object;
      assertEquals(size, array.size());
    }
  }
}