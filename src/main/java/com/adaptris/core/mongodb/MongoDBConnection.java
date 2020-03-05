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
import com.adaptris.core.AllowsRetriesConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.sql.SQLException;

/**
 * @author mwarman
 * @config mongodb-connection
 */
@XStreamAlias("mongodb-connection")
@AdapterComponent
@ComponentProfile(summary = "Connect to MongoDB,", tag = "connections,mongodb")
@DisplayOrder(order = {"connectionUri", "database"})
public class MongoDBConnection extends AllowsRetriesConnection {

  private String connectionUri;
  private String database;

  private transient MongoClient mongoClient;
  private transient MongoDatabase mongoDatabase;

  public MongoDBConnection(){
    //NOP
  }

  public MongoDBConnection(String connectionUri, String database){
    this();
    setConnectionUri(connectionUri);
    setDatabase(database);
  }

  @Override
  protected void prepareConnection() {
    //NOP
  }

  @Override
  protected void initConnection() throws CoreException {
    try {
      mongoClient = attemptConnect();
      mongoDatabase = mongoClient.getDatabase(getDatabase());
    } catch (Exception e) {
      throw new CoreException(e);
    }
  }

  @Override
  protected void startConnection() {
    //NOP
  }

  @Override
  protected void stopConnection() {
    //NOP
  }

  @Override
  protected void closeConnection() {
    try {
      if (mongoClient != null) {
        mongoClient.close();
      }
    } catch (Exception ignored) {
      // ignored
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

  /**
   * <p>
   * Initiate a connection to the database.
   * </p>
   *
   * @throws SQLException if connection fails after exhausting the specified number of retry attempts
   */
  private MongoClient attemptConnect() throws SQLException {
    int attemptCount = 0;
    MongoClient mongoClient = null;
    while (mongoClient == null) {
      attemptCount++;
      mongoClient = new MongoClient(new MongoClientURI(getConnectionUri()));
      if (mongoClient == null) {
        if (logWarning(attemptCount)) {
          log.warn("Connection attempt [{}] failed for {}", attemptCount, getConnectionUri());
        }
        if (connectionAttempts() != -1 && attemptCount >= connectionAttempts()) {
          log.error("Failed to make any JDBC Connections");
          throw new SQLException("Could not connect to {}", getConnectionUri());
        } else {
          log.trace(createLoggingStatement(attemptCount));
          try {
            Thread.sleep(connectionRetryInterval());
          } catch (InterruptedException e2) {
            throw new SQLException(e2);
          }
        }
      }
    }
    return mongoClient;
  }

}
