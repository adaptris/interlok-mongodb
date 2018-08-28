package com.adaptris.core.mongodb;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mwarman
 */
public class FlattenJsonOutputSettingsTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testOutput() throws Exception{
    Document doc = new Document()
        .append("i", 1)
        .append("l", 1L)
        .append("d", 23.4)
        .append("d128", new Decimal128(1L))
        .append("date", new Date(1532044800000L))
        ;
    final JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
    final Object object = jsonParser.parse(doc.toJson(new FlattenJsonOutputSettings().settings()));
    assertTrue(object instanceof JSONObject);
    HashMap<String, Object> result = (HashMap<String, Object>)object;
    assertEquals(1, result.get("i"));
    assertEquals(1, result.get("l"));
    assertEquals(23.4, result.get("d"));
    assertEquals(1, result.get("d128"));
    //because timezones
    assertTrue(result.get("date").toString().startsWith("2018-07-19T"));
  }

}