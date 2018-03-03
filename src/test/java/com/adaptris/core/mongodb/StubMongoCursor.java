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
