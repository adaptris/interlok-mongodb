package com.adaptris.core.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.adaptris.util.TimeInterval;

/**
 * @author mwarman
 */
public class MongoDBConnectionTest {

  @Test
  public void testConstructor() {
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

  @Test
  public void testConnectionFailure() {
    MongoDBConnection connection = new MongoDBConnection("mongodb://localhost:27017", "database");
    connection.setConnectionAttempts(5);
    connection.setConnectionRetryInterval(new TimeInterval(5L, TimeUnit.SECONDS));
    try {
      connection.initConnection();

      assertNotNull(connection.retrieveMongoClient());
      assertNotNull(connection.retrieveMongoDatabase());
    } catch (Exception e) {
      // expected (unless you actually fire-up a MongoDB instance
    }
  }

}
