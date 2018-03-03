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
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author mwarman
 */
public class MongoDBWriteProducerTest extends MongoDBCase {

  @SuppressWarnings("unchecked")
  @Test
  public void produce() throws Exception{
    MongoDBWriteProducer producer = new MongoDBWriteProducer();
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("[ {\"key\": 1},{\"key\": 2}]");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    Mockito.verify(collection, Mockito.times(2)).insertOne(Mockito.any());
    LifecycleHelper.stopAndClose(producer);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void produceNoArray() throws Exception{
    MongoDBWriteProducer producer = new MongoDBWriteProducer();
    producer.registerConnection(connection);
    producer.setSplitJsonArray(false);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"key\": 1}");
    LifecycleHelper.initAndStart(producer);
    producer.produce(msg, new ConfiguredDestination(COLLECTION));
    Mockito.verify(collection, Mockito.times(1)).insertOne(Mockito.any());
    LifecycleHelper.stopAndClose(producer);
  }


}