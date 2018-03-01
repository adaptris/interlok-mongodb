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
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author mwarman
 */
public class MongoDBWriteProducerTest {

  MongoDBConnection connection;
  MongoDatabase database;

  private static final String COLLECTION = "collection";
  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);
  private static final String filter = "{ \"stars\" : { \"$gte\" : 2, \"$lt\" : 5 }, \"categories\" : \"Bakery\" }";

  @Before
  public void before() throws Exception{
    connection = new MongoDBConnection("mongodb://127.0.0.1:27017", "test");
    database = connection.createDatabase(connection.createClient());
  }

  @After
  public void after() {
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    collection.deleteMany(Document.parse("{}"));
  }

  @Test
  public void produce() throws Exception{
    MongoDBWriteProducer producer = new MongoDBWriteProducer();
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    assertRecordsArePresent(2);
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void produceNoArray() throws Exception{
    MongoDBWriteProducer producer = new MongoDBWriteProducer();
    producer.registerConnection(connection);
    producer.setSplitJsonArray(false);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    assertRecordsArePresent(1);
    LifecycleHelper.stopAndClose(producer);
  }

  private void assertRecordsArePresent(int expected){
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    ArrayList<Document> results = collection.find().into(new ArrayList<>());
    assertEquals(expected, results.size());
  }
}