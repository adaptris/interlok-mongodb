package com.adaptris.core.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * @author mwarman
 */
public class FlattenJsonOutputSettingsTest {

  @Test
  public void testOutput() throws Exception {
    Document doc = new Document().append("i", 1).append("l", 1L).append("d", 23.4).append("d128", new Decimal128(1L)).append("date",
        new Date(1532044800000L));
    // final JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
    // final Object object = jsonParser.parse(doc.toJson(new FlattenJsonOutputSettings().settings()));

    final JSONObject object = new JSONObject(doc.toJson(new FlattenJsonOutputSettings().settings()));
    assertEquals(1, object.get("i"));
    assertEquals(1, object.get("l"));
    assertEquals(23.4, object.getDouble("d"), 0.1);
    assertEquals(1, object.get("d128"));
    // because timezones
    assertTrue(object.get("date").toString().startsWith("2018-07"));
  }

}