package com.adaptris.core.mongodb;

import org.bson.json.JsonWriterSettings;

/**
 * @author mwarman
 */
public interface JsonOutputSettings {

  JsonWriterSettings settings();
}
