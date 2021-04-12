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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.util.CloseableIterable;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Producer that executes aggregate MongoDB queries, results returned as JSON Array.
 *
 * <p>
 *   <b>Example Pipeline:</b><br/>
 *   Returns the count of stars
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
 *     [{ "$group" : { "_id" : "$stars", "count" : { "$sum" : 1 } } }]
 *     }
 *   </pre>
 * </p>
 * <p>
 *   Result:
 *   <pre>
 *     {@code
 *     [ { "_id" : "1", "count" : "1" }, { "_id" : "3", "count" : "1" } ]
 *     }
 *   </pre>
 * </p>
 * @author mwarman
 * @config mongodb-aggregate-producer
 */
@XStreamAlias("mongodb-aggregate-producer")
@AdapterComponent
@ComponentProfile(summary = "Executes aggregate MongoDB queries, results returned as JSON Array.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@DisplayOrder(order = {"collection", "allowDiskUse", "toCollection", "pipeline"})
@NoArgsConstructor
public class MongoDBAggregateProducer extends MongoDBRetrieveProducer {

  @Valid
  @NotNull
  private DataInputParameter<String> pipeline;

  @AdvancedConfig
  private Boolean allowDiskUse;

  @AdvancedConfig
  private Boolean toCollection;


  @Override
  protected MongoIterable<Document> retrieveResults(MongoCollection<Document> collection, AdaptrisMessage msg) throws InterlokException {
    AggregateIterable<Document> results = collection.aggregate(createPipeline(msg)).allowDiskUse(allowDiskUse());
    if(toCollection()) {
      results.toCollection();
      return null;
    } else {
      return results;
    }
  }


  private List<Bson> createPipeline(AdaptrisMessage message) throws InterlokException {
    List<Bson> results = new ArrayList<>();
    LargeJsonArraySplitter splitter = new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
    try (CloseableIterable<AdaptrisMessage> messages =
             CloseableIterable.ensureCloseable(
                 splitter.splitMessage(AdaptrisMessageFactory.getDefaultInstance().newMessage(getPipeline().extract(message)))
             )
    ) {
      for(AdaptrisMessage m : messages) {
        results.add(BsonDocument.parse(m.getContent()));
      }
    } catch (IOException e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
    return results;
  }

  public DataInputParameter<String> getPipeline() {
    return pipeline;
  }

  public void setPipeline(DataInputParameter<String> pipeline) {
    this.pipeline = pipeline;
  }

  public Boolean getAllowDiskUse() {
    return allowDiskUse;
  }

  public void setAllowDiskUse(Boolean allowDiskUse) {
    this.allowDiskUse = allowDiskUse;
  }

  boolean allowDiskUse(){
    return getAllowDiskUse() != null ? getAllowDiskUse() : false;
  }

  public Boolean getToCollection() {
    return toCollection;
  }

  public void setToCollection(Boolean toCollection) {
    this.toCollection = toCollection;
  }

  boolean toCollection(){
    return getToCollection() != null ? getToCollection() : false;
  }

  public MongoDBAggregateProducer withPipeline(DataInputParameter<String> filter) {
    setPipeline(filter);
    return this;
  }
}
