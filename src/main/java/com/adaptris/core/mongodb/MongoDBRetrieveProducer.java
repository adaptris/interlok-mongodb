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

import java.io.Writer;
import org.bson.Document;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import lombok.NoArgsConstructor;

/**
 * @author mwarman
 */
@NoArgsConstructor
public abstract class MongoDBRetrieveProducer extends MongoDBProducer {

  @AdvancedConfig
  private Integer batchSize;

  @AdvancedConfig
  private JsonOutputSettings jsonOutputSettings = new DefaultJsonOutputSettings();


  @Override
  protected final AdaptrisMessage doRequest(AdaptrisMessage msg, String collectionName,
      long timeout, AdaptrisMessage reply) throws ProduceException {
    ObjectMapper mapper = new ObjectMapper();
    try (Writer w = reply.getWriter();
         JsonGenerator generator = mapper.getFactory().createGenerator(w).useDefaultPrettyPrinter()) {
      MongoCollection<Document> collection = getMongoDatabase().getCollection(collectionName);
      generator.writeStartArray();
      MongoIterable<Document> results = retrieveResults(collection, msg);
      if(results != null) {
        if (getBatchSize() != null) {
          results.batchSize(getBatchSize());
        }
        for (Document document : results) {
          generator.writeRawValue(document.toJson(getJsonOutputSettings().settings()));
        }
      }
      generator.writeEndArray();
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    log.trace(String.format("Response size [%s]", reply.getSize()));
    return reply;
  }

  protected abstract MongoIterable<Document> retrieveResults(MongoCollection<Document> collection, AdaptrisMessage msg) throws InterlokException;

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public JsonOutputSettings getJsonOutputSettings() {
    return jsonOutputSettings;
  }

  public void setJsonOutputSettings(JsonOutputSettings jsonOutputSettings) {
    this.jsonOutputSettings = jsonOutputSettings;
  }
}
