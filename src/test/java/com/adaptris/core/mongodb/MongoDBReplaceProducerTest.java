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
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author mwarman
 */
@SuppressWarnings("deprecation")
public class MongoDBReplaceProducerTest extends MongoDBCase {


  @Override
  @Before
  public void onSetup() throws Exception {
    Document document = new Document("name", "Café Con Leche")
        .append("stars", 3)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
    Document document2 = new Document("name", "Fred's")
        .append("stars", 1)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));

    if(localTests){
      collection.insertMany(Arrays.asList(document, document2));
    } else {

    }
  }

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Test
  public void testProduceUpsertArrayNoData() throws Exception{
    clearData();
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("key"));
    producer.registerConnection(connection);
    producer.setUpsert(true);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(2);
    } else {
      verifyUpdateCounts(2, true);
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceUpsertNoArrayNoData() throws Exception{
    clearData();
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("key"));
    producer.registerConnection(connection);
    producer.setUpsert(true);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(1);
    } else {
      verifyUpdateCounts(1, true);
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceNoUpsertArrayNoData() throws Exception{
    clearData();
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("key"));
    producer.registerConnection(connection);
    producer.setUpsert(false);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(0);
    } else {
      verifyUpdateCounts(2, false);
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceNoUpsertNoArrayNoData() throws Exception{
    clearData();
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("key"));
    producer.registerConnection(connection);
    producer.setUpsert(false);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(0);
    } else {
      verifyUpdateCounts(1, false);
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceUpsertNoArrayData() throws Exception{
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("name"));
    producer.registerConnection(connection);
    producer.setUpsert(true);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"name\": \"Fred's\", \"stars\": 2}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(2);
    } else {
      verifyUpdateCounts(1, true);
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceUpsertNoArrayDataValueConverter() throws Exception{
    MongoDBReplaceProducer producer = new MongoDBReplaceProducer().withCollection(COLLECTION);
    producer.setFilterFields(Collections.singletonList("name"));
    producer.registerConnection(connection);
    producer.setUpsert(true);
    producer.setValueConverters(Collections.singletonList(new IntegerValueConverter("stars")));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"name\": \"Fred's\", \"stars\": \"2\"}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if(localTests){
      assertRecordsArePresent(2);
    } else {
      verifyUpdateCounts(1, true);
    }
    LifecycleHelper.stopAndClose(producer);
  }


  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(
        new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBReplaceProducer()
        .withUpsert(true)
        .withFilterFields(Collections.singletonList("_id"))
            .withCollection("collection")
        );
  }

  private void assertRecordsArePresent(int expected){
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    ArrayList<Document> results = collection.find().into(new ArrayList<>());
    assertEquals(expected, results.size());
  }

  private void verifyUpdateCounts(int expected, boolean upsert) {
    ArgumentCaptor<ReplaceOptions> argument = ArgumentCaptor.forClass(ReplaceOptions.class);
    Mockito.verify(collection, Mockito.times(expected)).replaceOne(Mockito.any(Bson.class), Mockito.any(), argument.capture());
    assertEquals(expected, argument.getAllValues().size());
    for (ReplaceOptions u : argument.getAllValues()) {
      assertEquals(upsert, u.isUpsert());
    }
  }
}
