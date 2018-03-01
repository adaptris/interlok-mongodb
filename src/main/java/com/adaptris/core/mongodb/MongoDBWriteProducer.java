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

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.*;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.mongodb.client.MongoCollection;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.Document;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

/**
 * @author mwarman
 */
@XStreamAlias("mongodb-write-producer")
public class MongoDBWriteProducer extends MongoDBProducer {

  @AdvancedConfig
  @InputFieldDefault(value = "true")
  private Boolean splitJsonArray;

  public MongoDBWriteProducer(){
  }


  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout, AdaptrisMessage reply) throws ProduceException {
    try {
      MongoCollection<Document> collection = getMongoDatabase().getCollection(destination.getDestination(msg));
      if (splitJsonArray()) {
        LargeJsonArraySplitter splitter = new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
        for (AdaptrisMessage m : splitter.splitMessage(msg)) {
          parseAndSendDocument(collection, m);
        }
      } else {
        parseAndSendDocument(collection, msg);
      }
      return reply;
    } catch (CoreException e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private void parseAndSendDocument(MongoCollection<Document> collection, AdaptrisMessage message){
    Document document = Document.parse(message.getContent());
    collection.insertOne(document);
  }

  private boolean splitJsonArray() {
    return getSplitJsonArray() != null ? getSplitJsonArray().booleanValue() : true;
  }

  public void setSplitJsonArray(Boolean splitJsonArray) {
    this.splitJsonArray = splitJsonArray;
  }

  public Boolean getSplitJsonArray(){
    return splitJsonArray;
  }

  public MongoDBWriteProducer withJsonArray(Boolean jsonArray) {
    this.setSplitJsonArray(jsonArray);
    return this;
  }
}
