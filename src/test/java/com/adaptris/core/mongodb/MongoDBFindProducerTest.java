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
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author mwarman
 */
public class  MongoDBFindProducerTest extends MongoDBCase {

  private static final String FILTER = "{ \"stars\" : { \"$gte\" : 2, \"$lt\" : 5 }, \"categories\" : \"Bakery\" }";

  @Before
  public void setUp() throws Exception{
    super.setUp();

    FindIterable allIterable = mock(FindIterable.class);
    FindIterable filteredIterable = mock(FindIterable.class);

    doReturn(allIterable).when(collection).find(new Document());
    doReturn(filteredIterable).when(collection).find(Document.parse(FILTER));

    Document document = new Document("name", "Café Con Leche")
        .append("stars", 3)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
    Document document2 = new Document("name", "Fred's")
        .append("stars", 1)
        .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));

    doReturn(new StubMongoCursor(Arrays.asList(document, document2))).when(allIterable).iterator();
    doReturn(new StubMongoCursor(Collections.singletonList(document))).when(filteredIterable).iterator();
  }

  @Test
  public void testFilter() {
    MongoDBFindProducer producer = new MongoDBFindProducer();
    assertNull(producer.getFilter());
    DataInputParameter<String> parameter = new StringPayloadDataInputParameter();
    producer = new MongoDBFindProducer().withFilter(parameter);
    assertEquals(parameter, producer.getFilter());
    producer = new MongoDBFindProducer();
    producer.setFilter(parameter);
    assertEquals(parameter, producer.getFilter());
  }

  @Test
  public void testDoRequest() throws Exception{
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
  public void testDoRequestWithFilter() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer().withFilter(new StringPayloadDataInputParameter());
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(FILTER);
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    assertJsonArraySize(result, 1);
    LifecycleHelper.stopAndClose(producer);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(
        new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBFindProducer()
            .withFilter(new ConstantDataInputParameter(FILTER))
            .withDestination(new ConfiguredDestination("collection"))
    );
  }
}