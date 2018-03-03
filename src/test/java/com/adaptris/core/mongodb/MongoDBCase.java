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

import com.adaptris.util.TimeInterval;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author mwarman
 */
public abstract class MongoDBCase {

  MongoDBConnection connection;
  MongoDatabase database;

  static final String COLLECTION = "collection";

  static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  @Before
  public void before() throws Exception{
    connection = new MongoDBConnection("mongodb://127.0.0.1:27017", "test");
    database = connection.createDatabase(connection.createClient());
  }

  @After
  public void after() {
    MongoCollection<Document> collection = database.getCollection("collection");
    collection.deleteMany(Document.parse("{}"));
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

  void assertRecordsArePresent(int expected){
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    ArrayList<Document> results = collection.find().into(new ArrayList<>());
    assertEquals(expected, results.size());
  }

}