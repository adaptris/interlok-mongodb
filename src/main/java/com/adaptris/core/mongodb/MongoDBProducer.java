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

import com.adaptris.core.*;
import com.adaptris.util.TimeInterval;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.concurrent.TimeUnit;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

/**
 * @author mwarman
 */
public abstract class MongoDBProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  private transient MongoClient mongoClient = null;
  private transient MongoDatabase mongoDatabase = null;

  @Override
  public void prepare() {
    //NOP
  }

  @Override
  public void init() throws CoreException {
    MongoDBConnection connection = retrieveConnection(MongoDBConnection.class);
    mongoClient = connection.retrieveMongoClient();
    mongoDatabase = connection.retrieveMongoDatabase();
  }

  @Override
  public void start() {
    //NOP
  }

  @Override
  public void stop() {
    //NOP
  }

  @Override
  public void close() {
    //NOP
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    return doRequest(msg, destination, timeout, msg);
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    doRequest(msg, destination, defaultTimeout(), defaultIfNull(getMessageFactory()).newMessage());
  }

  protected abstract AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout, AdaptrisMessage reply) throws ProduceException;

  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  protected MongoClient getMongoClient(){
    return this.mongoClient;
  }

  protected MongoDatabase getMongoDatabase(){
    return this.mongoDatabase;
  }

}
