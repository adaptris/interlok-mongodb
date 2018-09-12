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

import com.adaptris.core.*;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * @author mwarman
 */
public abstract class MongoDBArrayProducer extends MongoDBProducer {

  private static final Integer DEFAULT_BUFFER_SIZE = 8192;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout, AdaptrisMessage reply) throws ProduceException {
    try {
      MongoCollection<Document> collection = getMongoDatabase().getCollection(destination.getDestination(msg));
      if (isJsonArray(msg)) {
        LargeJsonArraySplitter splitter = new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
        for (AdaptrisMessage m : splitter.splitMessage(msg)) {
          parseAndActionDocument(collection, m);
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
