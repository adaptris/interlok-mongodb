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
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.bson.Document;

import javax.validation.Valid;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Producer that inserts JSON objects into MongoDB, if a JSON array is given the array will be split and inserted as individual JSON objects.
 *
 * @author mwarman
 * @config mongodb-write-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Inserts JSON objects into MongoDB.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@XStreamAlias("mongodb-write-producer")
public class MongoDBWriteProducer extends MongoDBArrayProducer {

  @Valid
  @XStreamImplicit
  private List<ValueConverter> valueConverters = new ArrayList<>();

  public MongoDBWriteProducer(){
    //NOP
  }

  public void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message){
    Document document = Document.parse(message.getContent());
    for(ValueConverter valueConverter : getValueConverters()){
      document.put(valueConverter.key(), valueConverter.convert(document));
    }
    collection.insertOne(document);
    log.trace("Record Inserted");
  }

  public List<ValueConverter> getValueConverters() {
    return valueConverters;
  }

  public void setValueConverters(List<ValueConverter> valueConverters) {
    this.valueConverters = valueConverters;
  }

  public MongoDBWriteProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }

}
