package com.adaptris.core.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mwarman
 */
public class MongoDBConnectionTest {

  @Test
  public void testConstructor(){
    MongoDBConnection connection = new MongoDBConnection();
    assertNull(connection.getConnectionUri());
    assertNull(connection.getDatabase());
    connection = new MongoDBConnection("mongodb://localhost:27017", "database");
    assertEquals("mongodb://localhost:27017", connection.getConnectionUri());
    assertEquals("database", connection.getDatabase());
  }

  @Test
  public void testGetConnectionUri() {
    MongoDBConnection connection = new MongoDBConnection();
    assertNull(connection.getConnectionUri());
    connection.setConnectionUri("mongodb://localhost:27017");
    assertEquals("mongodb://localhost:27017", connection.getConnectionUri());
  }

  @Test
  public void testGetDatabase() {
    MongoDBConnection connection = new MongoDBConnection();
    assertNull(connection.getDatabase());
    connection.setDatabase("database");
    assertEquals("database", connection.getDatabase());
  }
}