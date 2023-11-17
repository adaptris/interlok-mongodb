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

import java.util.ArrayList;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.mongodb.client.MongoCollection;

/**
 * @author mwarman
 */
public class MongoDBWriteProducerTest extends MongoDBCase {

  @Test
  public void testProduce() throws Exception {
    MongoDBWriteProducer producer = new MongoDBWriteProducer().withCollection(COLLECTION);
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if (localTests) {
      assertRecordsArePresent(2);
    } else {
      Mockito.verify(collection, Mockito.times(2)).insertOne(Mockito.any());
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testProduceNoArray() throws Exception {
    MongoDBWriteProducer producer = new MongoDBWriteProducer().withCollection(COLLECTION);
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg);
    if (localTests) {
      assertRecordsArePresent(1);
    } else {
      Mockito.verify(collection, Mockito.times(1)).insertOne(Mockito.any());
    }
    LifecycleHelper.stopAndClose(producer);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBWriteProducer().withCollection("collection"));
  }

  private void assertRecordsArePresent(int expected) {
    MongoCollection<Document> collection = database.getCollection(COLLECTION);
    ArrayList<Document> results = collection.find().into(new ArrayList<>());
    assertEquals(expected, results.size());
  }

}
