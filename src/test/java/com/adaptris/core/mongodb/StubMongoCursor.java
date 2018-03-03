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

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mwarman
 */
public class StubMongoCursor implements MongoCursor<Document> {

  List<Document> documents = new ArrayList<>();
  private int curPos=0;

  public StubMongoCursor(List<Document> documents){
    this.documents.addAll(documents);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNext() {
    return documents.size() > curPos;
  }

  @Override
  public Document next() {
    return documents.get(curPos++);
  }

  @Override
  public Document tryNext() {
    return null;
  }

  @Override
  public ServerCursor getServerCursor() {
    return null;
  }

  @Override
  public ServerAddress getServerAddress() {
    return null;
  }
}
