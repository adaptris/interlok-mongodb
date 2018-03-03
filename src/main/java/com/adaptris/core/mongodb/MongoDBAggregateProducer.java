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
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mwarman
 */
@XStreamAlias("mongodb-aggregate-producer")
public class MongoDBAggregateProducer extends MongoDBRetrieveProducer {

  public MongoDBAggregateProducer() {
  }

  @Override
  protected MongoIterable<Document> retrieveResults(MongoCollection<Document> collection, AdaptrisMessage msg) throws InterlokException {
    return collection.aggregate(createFilter(msg));
  }


  private List<Bson> createFilter(AdaptrisMessage message) throws InterlokException {
    List<Bson> results = new ArrayList<>();
    LargeJsonArraySplitter splitter = new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
    for (AdaptrisMessage m : splitter.splitMessage(message)) {
      results.add(BsonDocument.parse(m.getContent()));
    }
    return results;
  }

  public MongoDBAggregateProducer withFilter(DataInputParameter<String> filter) {
    setFilter(filter);
    return this;
  }
}
