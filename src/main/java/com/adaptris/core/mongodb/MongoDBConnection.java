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

import com.adaptris.core.CoreException;
import com.adaptris.core.NoOpConnection;
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
public class MongoDBConnection extends NoOpConnection{

  String connectionUri;
  String database;

  public MongoDBConnection(){
  }

  public MongoDBConnection(String connectionUri, String database){
    this();
    setConnectionUri(connectionUri);
    setDatabase(database);
  }

  protected MongoClient createClient() throws CoreException {
    return new MongoClient(new MongoClientURI(getConnectionUri()));
  }

  protected MongoDatabase createDatabase(MongoClient client) throws CoreException {
    return client.getDatabase(getDatabase());
  }

  protected void closeQuietly(MongoClient c) {
    try {
      if (c != null) {
        c.close();
      }
    }
    catch (Exception ignored) {
      ;
    }
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
