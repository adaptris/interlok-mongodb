package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.json.JsonWriterSettings;

/**
 * @author mwarman
 * @config mongodb-default-json-output-settings
 */
@XStreamAlias("mongodb-default-json-output-settings")
public class DefaultJsonOutputSettings implements JsonOutputSettings {
  private final transient JsonWriterSettings settings;

  public DefaultJsonOutputSettings(){
    settings = JsonWriterSettings.builder().build();
  }

  @Override
  public JsonWriterSettings settings() {
    return settings;
  }
}
