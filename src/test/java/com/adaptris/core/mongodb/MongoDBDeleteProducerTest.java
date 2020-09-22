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

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredDestination;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.mongodb.client.MongoCollection;

/**
 * @author mwarman
 */
@SuppressWarnings("deprecation")
public class MongoDBDeleteProducerTest extends MongoDBCase {

  @Override
  @Before
  public void onSetup() throws Exception {
    if (localTests) {
      Document document = new Document("key", 1);
      Document document2 = new Document("key", 2);
      collection.insertMany(Arrays.asList(document, document2));
    }
  }

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Test
  public void testProduce() throws Exception{
    MongoDBDeleteProducer producer = new MongoDBDeleteProducer().withCollection(COLLECTION);
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    if(localTests){
      assertRecordsArePresent(0);
    } else {
      Mockito.verify(collection, Mockito.times(2)).deleteOne(Mockito.any());
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceNoArray() throws Exception{
    MongoDBDeleteProducer producer = new MongoDBDeleteProducer().withCollection(COLLECTION);
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    if(localTests){
      assertRecordsArePresent(1);
    } else {
      Mockito.verify(collection, Mockito.times(1)).deleteOne(Mockito.any());
    }
    LifecycleHelper.stopAndClose(producer);
  }


  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(
        new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBDeleteProducer()
            .withCollection("collection")
        );
  }

  private void assertRecordsArePresent(int expected){
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    ArrayList<Document> results = collection.find().into(new ArrayList<>());
    assertEquals(expected, results.size());
  }
}