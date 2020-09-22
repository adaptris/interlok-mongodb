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

import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import org.bson.Document;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.mongodb.client.MongoCollection;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import lombok.NoArgsConstructor;

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
@DisplayOrder(order = {"collection"})
@NoArgsConstructor
public class MongoDBWriteProducer extends MongoDBArrayProducer {

  @Valid
  @XStreamImplicit
  private List<ValueConverter> valueConverters = new ArrayList<>();


  @Override
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

}
