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
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.BsonDocument;
import org.bson.Document;

import javax.validation.Valid;

/**
 * Producer that executes find MongoDB queries, results returned as JSON Array.
 *
 * <p>
 *   <b>Example Filter:</b><br/>
 *   Filters results stars greater than or equal to 2 and less that 5 with category of Bakery.
 * </p>
 * <p>
 *   Data:
 *   <pre>
 *     {@code
 *     [
 *       { "name" : "Café Con Leche", "stars" : 3, "categories" : ["Bakery", "Coffee", "Pastries"] },
 *       { "name" : "Fred's", "stars" : 1, "categories" : ["Bakery", "Coffee", "Pastries"] }
 *     ]
 *     }
 *   </pre>
 * </p>
 * <p>
 *   Query:
 *   <pre>
 *     {@code
 *     { "stars" : { "$gte" : 2, "$lt" : 5 }, "categories" : "Bakery" }
 *     }
 *   </pre>
 * </p>
 * <p>
 *   Result:
 *   <pre>
 *     {@code
 *     [ { "name" : "Café Con Leche", "stars" : 3, "categories" : ["Bakery", "Coffee", "Pastries"] } ]
 *     }
 *   </pre>
 * </p>
 * @author mwarman
 * @config mongodb-find-producer
 */
@XStreamAlias("mongodb-find-producer")
@AdapterComponent
@ComponentProfile(summary = "Executes find MongoDB queries, results returned as JSON Array.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@DisplayOrder(order = {"filter", "sort", "limit"})
public class MongoDBFindProducer extends MongoDBRetrieveProducer {

  @Valid
  private DataInputParameter<String> filter;

  @Valid
  private DataInputParameter<String> sort;

  @Valid
  private DataInputParameter<String> projection;

  @Valid
  @InputFieldHint(expression = true)
  private String limit;

  public MongoDBFindProducer() {
    //NOP
  }

  @Override
  protected MongoIterable<Document> retrieveResults(MongoCollection<Document> collection, AdaptrisMessage msg) throws InterlokException {
    FindIterable<Document> iterable =  collection.find(createFilter(msg));
    if(getProjection() != null){
      iterable = iterable.projection(BsonDocument.parse(getProjection().extract(msg)));
    }
    if (getSort() != null){
      iterable  = iterable.sort(BsonDocument.parse(getSort().extract(msg)));
    }
    if (getLimit() !=  null){
      iterable = iterable.limit(Integer.parseInt(msg.resolve(getLimit())));
    }
    return iterable;
  }

  private Document createFilter(AdaptrisMessage message) throws InterlokException {
    return getFilter() != null ? Document.parse(getFilter().extract(message)) : new Document();
  }

  public DataInputParameter<String> getFilter() {
    return filter;
  }

  public void setFilter(DataInputParameter<String> filter) {
    this.filter = filter;
  }

  public DataInputParameter<String> getSort() {
    return sort;
  }

  public void setSort(DataInputParameter<String> sort) {
    this.sort = sort;
  }

  public String getLimit() {
    return limit;
  }

  public void setLimit(String limit) {
    this.limit = limit;
  }

  public DataInputParameter<String> getProjection() {
    return projection;
  }

  public void setProjection(DataInputParameter<String> projection) {
    this.projection = projection;
  }

  public MongoDBFindProducer withFilter(DataInputParameter<String> filter) {
    setFilter(filter);
    return this;
  }

  public MongoDBFindProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }

  public MongoDBFindProducer withSort(DataInputParameter<String> sort){
    setSort(sort);
    return this;
  }

  public MongoDBFindProducer withLimit(String limit){
    setLimit(limit);
    return this;
  }

}
