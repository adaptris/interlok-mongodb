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

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredDestination;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;
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
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author mwarman
 */
public class MongoDBFindProducerTest {

  MongoDBConnection connection;
  MongoDatabase database;

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  private static final String filter = "{ \"stars\" : { \"$gte\" : 2, \"$lt\" : 5 }, \"categories\" : \"Bakery\" }";

  @Before
  public void before() throws Exception{
    connection = new MongoDBConnection("mongodb://127.0.0.1:27017", "test");
    database = connection.createDatabase(connection.createClient());
    MongoCollection<Document> collection = database.getCollection("collection");
    Document document = new Document("name", "Café Con Leche")
        .append("stars", 3)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
    Document document2 = new Document("name", "Fred's")
        .append("stars", 1)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
    collection.insertMany(Arrays.asList(document, document2));
  }

  @After
  public void after() {
    MongoCollection<Document> collection = database.getCollection("collection");
    collection.deleteMany(Document.parse("{}"));
  }

  @Test
  public void doRequest() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer();
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    assertJsonArraySize(result, 2);
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void doRequestWithFilter() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer(new StringPayloadDataInputParameter());
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(filter);
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    assertJsonArraySize(result, 1);
    LifecycleHelper.stopAndClose(producer);
  }

  private void assertJsonArraySize(String contents, int size) throws ParseException {
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