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

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.util.TimeInterval;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import jakarta.validation.constraints.NotBlank;
import java.util.concurrent.TimeUnit;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

/**
 * @author mwarman
 */
@NoArgsConstructor
public abstract class MongoDBProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  private transient MongoClient mongoClient = null;
  private transient MongoDatabase mongoDatabase = null;

  /**
   * The MongoDB collection name.
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String collection;

  @Override
  public void prepare() {
    Args.notBlank(getCollection(), "collection");
  }

  @Override
  public void init() throws CoreException {
    MongoDBConnection connection = retrieveConnection(MongoDBConnection.class);
    mongoClient = connection.retrieveMongoClient();
    mongoDatabase = connection.retrieveMongoDatabase();
  }


  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String endpoint, long timeout)
      throws ProduceException {
    return doRequest(msg, endpoint, timeout, msg);
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint)
      throws ProduceException {
    doRequest(msg, endpoint, defaultTimeout(), defaultIfNull(getMessageFactory()).newMessage());
  }

  protected abstract AdaptrisMessage doRequest(AdaptrisMessage msg, String collection, long timeout,
      AdaptrisMessage reply) throws ProduceException;

  @Override
  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  protected MongoClient getMongoClient(){
    return mongoClient;
  }

  protected MongoDatabase getMongoDatabase(){
    return mongoDatabase;
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getCollection());
  }

  @SuppressWarnings("unchecked")
  public <T extends MongoDBProducer> T withCollection(String s) {
    setCollection(s);
    return(T) this;
  }
}
