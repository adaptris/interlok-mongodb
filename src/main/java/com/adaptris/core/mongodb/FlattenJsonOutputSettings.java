package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.json.Converter;
import org.bson.json.JsonWriterSettings;
import org.bson.json.StrictJsonWriter;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author mwarman
 */
@XStreamAlias("mongodb-flatten-json-output-settings")
public class FlattenJsonOutputSettings implements JsonOutputSettings {

  private final transient JsonWriterSettings settings;

  public FlattenJsonOutputSettings(){
    settings = JsonWriterSettings.builder()
        .objectIdConverter((value, writer) -> writer.writeString(value.toString()))
        .doubleConverter((value, writer) -> writer.writeNumber(value.toString()))
        .int32Converter((value, writer) -> writer.writeNumber(value.toString()))
        .int64Converter((value, writer) -> writer.writeNumber(value.toString()))
        .decimal128Converter((value, writer) -> writer.writeNumber(value.toString()))
        .dateTimeConverter((value, writer) -> writer.writeString(new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ").format(new Date(value))))
        .build();
  }

  @Override
  public JsonWriterSettings settings() {
    return settings;
  }
}
