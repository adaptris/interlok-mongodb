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

import org.bson.Document;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

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
@DisplayOrder(order = {"collection"})
@NoArgsConstructor
public class MongoDBDeleteProducer extends MongoDBArrayProducer {

  @Override
  public void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message){
    Document document = Document.parse(message.getContent());
    DeleteResult result = collection.deleteOne(document);
    log.trace(result.toString());
  }
}
