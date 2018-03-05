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
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.AggregateIterable;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author mwarman
 */
public class MongoDBAggregateProducerTest extends MongoDBCase {

  private static final String PIPELINE = "[{ \"$group\" : { \"_id\" : \"$stars\", \"count\" : { \"$sum\" : 1 } } }]";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception{
    super.setUp();

    AggregateIterable iterable = mock(AggregateIterable.class);

    doReturn(iterable).when(collection).aggregate(any());

    Document document = new Document("_id", "1").append("count", "1");
    Document document2 = new Document("_id", "3").append("count", "1");

    doReturn(new StubMongoCursor(Arrays.asList(document, document2))).when(iterable).iterator();
  }

  @Test
  public void testPipeline() {
    MongoDBAggregateProducer producer = new MongoDBAggregateProducer();
    assertNull(producer.getPipeline());
    DataInputParameter<String> parameter = new StringPayloadDataInputParameter();
    producer = new MongoDBAggregateProducer().withPipeline(parameter);
    assertEquals(parameter, producer.getPipeline());
    producer = new MongoDBAggregateProducer();
    producer.setPipeline(parameter);
    assertEquals(parameter, producer.getPipeline());
  }


  @Test
  public void testDoRequest() throws Exception{
    MongoDBAggregateProducer producer = new MongoDBAggregateProducer().withPipeline(new StringPayloadDataInputParameter());
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(PIPELINE);
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    assertJsonArraySize(result, 2);
    LifecycleHelper.stopAndClose(producer);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(
        new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBAggregateProducer()
            .withPipeline(new ConstantDataInputParameter(PIPELINE))
            .withDestination(new ConfiguredDestination("collection"))
    );
  }
}
