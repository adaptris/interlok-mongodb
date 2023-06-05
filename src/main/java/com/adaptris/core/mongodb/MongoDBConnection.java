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

import java.sql.SQLException;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AllowsRetriesConnection;
import com.adaptris.core.CoreException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * @author mwarman
 * @config mongodb-connection
 */
@XStreamAlias("mongodb-connection")
@AdapterComponent
@ComponentProfile(summary = "Connect to MongoDB,", tag = "connections,mongodb")
@DisplayOrder(order = { "connectionUri", "database" })
public class MongoDBConnection extends AllowsRetriesConnection {

  /**
   * The URI of the database to connnect to
   */
  @NonNull
  @Getter
  @Setter
  private String connectionUri;
  /**
   * The name of the database to connnect to
   */
  @NonNull
  @Getter
  @Setter
  private String database;

  private transient MongoClient mongoClient;
  private transient MongoDatabase mongoDatabase;

  public MongoDBConnection() {
    // NOP
  }

  public MongoDBConnection(String connectionUri, String database) {
    this();
    setConnectionUri(connectionUri);
    setDatabase(database);
  }

  @Override
  protected void prepareConnection() {
    // NOP
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
    // NOP
  }

  @Override
  protected void stopConnection() {
    // NOP
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

  /**
   * <p>
   * Initiate a connection to the database.
   * </p>
   *
   * @throws SQLException
   *           if connection fails after exhausting the specified number of retry attempts
   */
  private MongoClient attemptConnect() throws SQLException {
    int attemptCount = 0;

    MongoDBServerConnection connection = new MongoDBServerConnection(new ServerAddress(connectionUri));
    MongoClient mongoClient = connection.getClient();

    while (!connection.isConnected()) {
      attemptCount++;

      if (logWarning(attemptCount)) {
        log.warn("Connection attempt [{}] failed for {}", attemptCount, connectionUri);
      }

      if (connectionAttempts() != -1 && attemptCount >= connectionAttempts()) {
        log.error("Failed to make connection to MongoDB instance");
        throw new SQLException("Could not connect to " + connectionUri);
      } else {
        log.trace(createLoggingStatement(attemptCount));
        try {
          Thread.sleep(connectionRetryInterval());
        } catch (InterruptedException e2) {
          throw new SQLException(e2);
        }
      }

    }
    return mongoClient;
  }

  class MongoDBServerConnection implements ServerMonitorListener {
    private MongoClient client;
    private boolean connected = false;

    public MongoDBServerConnection(ServerAddress serverAddress) {
      try {
        MongoClientOptions clientOptions = new MongoClientOptions.Builder().addServerMonitorListener(this).build();
        client = new MongoClient(serverAddress, clientOptions);
      } catch (Exception ex) {

      }
    }

    @Override
    public void serverHearbeatStarted(ServerHeartbeatStartedEvent serverHeartbeatStartedEvent) {
      // Ping Started
    }

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent serverHeartbeatSucceededEvent) {
      connected = true;
    }

    @Override
    public void serverHeartbeatFailed(ServerHeartbeatFailedEvent serverHeartbeatFailedEvent) {
      connected = false;
    }

    public boolean isConnected() {
      return connected;
    }

    public MongoClient getClient() {
      return client;
    }
  }

}
