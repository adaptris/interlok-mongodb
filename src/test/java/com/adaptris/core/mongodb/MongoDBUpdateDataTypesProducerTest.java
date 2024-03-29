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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

/**
 * @author mwarman
 */
@SuppressWarnings("deprecation")
public class MongoDBUpdateDataTypesProducerTest extends MongoDBCase {

  private static final String FILTER = "{ \"stars\" : { \"$gte\" : 2, \"$lt\" : 5 }, \"categories\" : \"Bakery\" }";

  FindIterable<Document> allIterable;

  @Override
  @BeforeEach
  public void onSetup() throws Exception {
    Document document = new Document("name", "Café Con Leche").append("stars", 3).append("string", 1).append("date", "2018-01-01")
        .append("milliseconds", "2018-01-01").append("dateAsDate", new Date()).append("dateNull", null).append("integer", "1")
        .append("alreadyInteger", 1).append("double", "1.1").append("decimal128", "1.1").append("long", "1")
        .append("hidden", new Document().append("value", "1")).append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
    Document document2 = new Document("name", "Fred's").append("stars", 1).append("categories",
        Arrays.asList("Bakery", "Coffee", "Pastries"));

    if (localTests) {
      collection.insertMany(Arrays.asList(document, document2));
    } else {
      allIterable = mock(FindIterable.class);
      FindIterable<Document> filteredIterable = mock(FindIterable.class);

      doReturn(allIterable).when(collection).find(new Document());
      doReturn(filteredIterable).when(collection).find(Document.parse(FILTER));

      doReturn(new StubMongoCursor(Arrays.asList(document, document2))).when(allIterable).iterator();
      doReturn(new StubMongoCursor(Collections.singletonList(document))).when(filteredIterable).iterator();

    }
  }

  @Test
  public void testFilter() {
    MongoDBUpdateDataTypesProducer producer = new MongoDBUpdateDataTypesProducer();
    assertNull(producer.getFilter());
    DataInputParameter<String> parameter = new StringPayloadDataInputParameter();
    producer = new MongoDBUpdateDataTypesProducer().withFilter(parameter);
    assertEquals(parameter, producer.getFilter());
    producer = new MongoDBUpdateDataTypesProducer();
    producer.setFilter(parameter);
    assertEquals(parameter, producer.getFilter());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDoRequest() throws Exception {
    MongoDBUpdateDataTypesProducer producer = new MongoDBUpdateDataTypesProducer().withCollection("collection");
    producer.withFilter(new ConstantDataInputParameter(FILTER));
    producer.withTypeConverters(Arrays.asList(new StringValueConverter("string"), new DateValueConverter("date", "yyyy-MM-dd"),
        new DateValueConverter("dateNull", "yyyy-MM-dd"), new MillisecondsValueConverter("dateAsDate", "yyyy-MM-dd"),
        new MillisecondsValueConverter("milliseconds", "yyyy-MM-dd"), new IntegerValueConverter("integer"),
        new IntegerValueConverter("alreadyInteger"), new LongValueConverter("long"), new DoubleValueConverter("double"),
        new Decimal128ValueConverter("decimal128"), new Decimal128ValueConverter("hidden.value")));
    producer.registerConnection(connection);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    LifecycleHelper.initAndStart(producer);
    producer.request(msg, TIMEOUT.toMilliseconds());
    Map<String, Object> result;
    if (!localTests) {
      verify(collection, times(1)).find(Document.parse(FILTER));
      ArgumentCaptor<Document> documents = ArgumentCaptor.forClass(Document.class);
      verify(collection, times(1)).updateOne(any(), documents.capture());
      assertEquals(1, documents.getAllValues().size());
      Document document = documents.getValue();
      assertTrue(document.containsKey("$set"));
      result = (Map<String, Object>) document.get("$set");
    } else {
      MongoCollection<Document> collection = database.getCollection(COLLECTION);
      ArrayList<Document> results = collection.find(Document.parse(FILTER)).into(new ArrayList<>());
      assertEquals(1, results.size());
      result = results.get(0);
    }
    assertTrue(result.get("string") instanceof String);
    assertTrue(result.get("date") instanceof Date);
    assertNull(result.get("dateNull"));
    assertTrue(result.get("integer") instanceof Integer);
    assertTrue(result.get("alreadyInteger") instanceof Integer);
    assertTrue(result.get("milliseconds") instanceof Long);
    assertTrue(result.get("long") instanceof Long);
    assertTrue(result.get("dateAsDate") instanceof Long);
    assertTrue(result.get("double") instanceof Double);
    assertTrue(result.get("decimal128") instanceof Decimal128);
    assertTrue(result.get("hidden.value") instanceof Decimal128);
    LifecycleHelper.stopAndClose(producer);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new StandaloneProducer(new MongoDBConnection("mongodb://localhost:27017", "database"),
        new MongoDBUpdateDataTypesProducer().withFilter(new ConstantDataInputParameter(FILTER))
            .withTypeConverters(Arrays.asList(new StringValueConverter("stars"))).withCollection("collection"));
  }

}
