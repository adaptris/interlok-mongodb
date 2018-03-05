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
import com.adaptris.core.ProduceDestination;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.Document;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Producer that executes find MongoDB queries, results returned as JSON Array.
 *
 * <p>
 *   Example Filter:
 *
 *   Filters results stars greater than or equal to 2 and less that 5 with category of Bakery.
 * </p>
 * <p>
 *   Data:
 *   <pre>
 *     {@code
 *     [
 *       { "name" : "Café Con Leche", "stars" : 3, "categories" : ["Bakery", "Coffee", "Pastries"] },
 *       { "name" : "Fred's", "stars" : 1, "categories" : ["Bakery", "Coffee", "Pastries"] } ]
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
public class MongoDBFindProducer extends MongoDBRetrieveProducer {

  @Valid
  private DataInputParameter<String> filter;

  public MongoDBFindProducer() {
  }

  @Override
  protected MongoIterable<Document> retrieveResults(MongoCollection<Document> collection, AdaptrisMessage msg) throws InterlokException {
    return collection.find(createFilter(msg));
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

  public MongoDBFindProducer withFilter(DataInputParameter<String> filter) {
    setFilter(filter);
    return this;
  }

  public MongoDBFindProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }

}
