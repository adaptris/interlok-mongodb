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

import java.io.BufferedReader;
import java.io.IOException;
import org.bson.Document;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.util.CloseableIterable;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import lombok.NoArgsConstructor;

/**
 * @author mwarman
 */
@NoArgsConstructor
public abstract class MongoDBArrayProducer extends MongoDBProducer {

  private static final Integer DEFAULT_BUFFER_SIZE = 8192;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String collectionName, long timeout,
      AdaptrisMessage reply) throws ProduceException {
    try {
      MongoCollection<Document> collection = getMongoDatabase().getCollection(collectionName);
      if (isJsonArray(msg)) {
        LargeJsonArraySplitter splitter = new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
        try (CloseableIterable<AdaptrisMessage> messages = CloseableIterable.ensureCloseable(splitter.splitMessage(msg))) {
          for(AdaptrisMessage m : messages) {
            parseAndActionDocument(collection, m);
          }
        }
      } else {
        parseAndActionDocument(collection, msg);
      }
      return reply;
    } catch (InterlokException | IOException e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private boolean isJsonArray(AdaptrisMessage msg) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    try (BufferedReader buf = new BufferedReader(msg.getReader(), DEFAULT_BUFFER_SIZE);
        JsonParser parser = mapper.getFactory().createParser(buf)) {
      return parser.nextToken() == JsonToken.START_ARRAY;
    }
  }

  public abstract void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message) throws InterlokException;

}
