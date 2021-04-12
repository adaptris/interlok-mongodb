package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config mongodb-long-value-converter
 */
@XStreamAlias("mongodb-long-value-converter")
public class LongValueConverter extends ValueConverter<Long> {

  public LongValueConverter(){
    super();
  }

  public LongValueConverter(String key){
    super(key);
  }

  @Override
  Long valueOf(Object o) {
    return Long.valueOf(String.valueOf(o));
  }
}
