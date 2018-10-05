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

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.*;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Producer that delete JSON objects from MongoDB, if a JSON array is given the array will be split and delete as individual JSON objects.
 *
 * @author mwarman
 * @config mongodb-write-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Delete JSON objects from MongoDB.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@XStreamAlias("mongodb-delete-producer")
public class MongoDBDeleteProducer extends MongoDBArrayProducer {

  public MongoDBDeleteProducer(){
    //NOP
  }

  public void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message){
    Document document = Document.parse(message.getContent());
    DeleteResult result = collection.deleteOne(document);
    log.trace(result.toString());
  }

  public MongoDBDeleteProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }

}
