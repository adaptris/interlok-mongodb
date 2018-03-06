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
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author mwarman
 */
public class  MongoDBFindProducerTest extends MongoDBCase {

  private static final String FILTER = "{ \"stars\" : { \"$gte\" : 2, \"$lt\" : 5 }, \"categories\" : \"Bakery\" }";
  private static final String SORT ="{\"stars\" : 1}";
  private static final Integer LIMIT = 5;

  FindIterable allIterable;

  @Before
  public void setUp() throws Exception{
    super.setUp();

    allIterable = mock(FindIterable.class);
    FindIterable filteredIterable = mock(FindIterable.class);

    doReturn(allIterable).when(collection).find(new Document());
    doReturn(filteredIterable).when(collection).find(Document.parse(FILTER));

    doReturn(allIterable).when(allIterable).sort(BsonDocument.parse(SORT));
    doReturn(allIterable).when(allIterable).limit(LIMIT);

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
  public void testSort() {
    MongoDBFindProducer producer = new MongoDBFindProducer();
    assertNull(producer.getSort());
    DataInputParameter<String> parameter = new StringPayloadDataInputParameter();
    producer = new MongoDBFindProducer().withSort(parameter);
    assertEquals(parameter, producer.getSort());
    producer = new MongoDBFindProducer();
    producer.setSort(parameter);
    assertEquals(parameter, producer.getSort());
  }

  @Test
  public void testLimit() {
    MongoDBFindProducer producer = new MongoDBFindProducer();
    assertNull(producer.getLimit());
    producer = new MongoDBFindProducer().withLimit(5);
    assertEquals(5, producer.getLimit().intValue());
    producer = new MongoDBFindProducer();
    producer.setLimit(5);
    assertEquals(5, producer.getLimit().intValue());
  }

  @Test
  public void testDoRequest() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer();
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    verify(collection, times(1)).find(new Document());
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
    verify(collection, times(1)).find(Document.parse(FILTER));
    assertJsonArraySize(result, 1);
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testDoRequestWithSort() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer().withSort(new ConstantDataInputParameter(SORT));
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    verify(collection, times(1)).find(new Document());
    verify(allIterable, times(1)).sort(BsonDocument.parse(SORT));
    assertJsonArraySize(result, 2);
    LifecycleHelper.stopAndClose(producer);
  }

  @Test
  public void testDoRequestWithLimit() throws Exception{
    MongoDBFindProducer producer = new MongoDBFindProducer().withLimit(LIMIT);
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    LifecycleHelper.initAndStart(producer);
    producer.doRequest(msg, new ConfiguredDestination("collection"), TIMEOUT.toMilliseconds());
    String result = msg.getContent();
    verify(collection, times(1)).find(new Document());
    verify(allIterable, times(1)).limit(LIMIT);
    assertJsonArraySize(result, 2);
    LifecycleHelper.stopAndClose(producer);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(
        new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBFindProducer()
            .withFilter(new ConstantDataInputParameter(FILTER))
            .withSort(new ConstantDataInputParameter("{\"stars\" : 1}"))
            .withLimit(5)
            .withDestination(new ConfiguredDestination("collection"))
    );
  }
}