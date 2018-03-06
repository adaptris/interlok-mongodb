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
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author mwarman
 * @config mongodb-connection
 */
@XStreamAlias("mongodb-connection")
@AdapterComponent
@ComponentProfile(summary = "Executes aggregate MongoDB queries, results returned as JSON Array.", tag = "connections,mongodb")
@DisplayOrder(order = {"connectionUri", "database"})
public class MongoDBConnection extends AdaptrisConnectionImp {

  private String connectionUri;
  private String database;

  private transient MongoClient mongoClient;
  private transient MongoDatabase mongoDatabase;

  public MongoDBConnection(){
  }

  public MongoDBConnection(String connectionUri, String database){
    this();
    setConnectionUri(connectionUri);
    setDatabase(database);
  }

  @Override
  protected void prepareConnection() throws CoreException {

  }

  @Override
  protected void initConnection() throws CoreException {
    mongoClient = new MongoClient(new MongoClientURI(getConnectionUri()));
    mongoDatabase = mongoClient.getDatabase(getDatabase());
  }

  @Override
  protected void startConnection() throws CoreException {

  }

  @Override
  protected void stopConnection() {

  }

  @Override
  protected void closeConnection() {
    try {
      if (mongoClient != null) {
        mongoClient.close();
      }
    }
    catch (Exception ignored) {
    }

  }

  protected MongoClient retrieveMongoClient() {
    return mongoClient;
  }

  protected MongoDatabase retrieveMongoDatabase() {
    return mongoDatabase;
  }


  public String getConnectionUri() {
    return connectionUri;
  }

  public void setConnectionUri(String connectionUri) {
    this.connectionUri = Args.notNull(connectionUri, "Connection URI");
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = Args.notNull(database, "Database");
  }
}
