package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config mongodb-string-value-converter
 */
@XStreamAlias("mongodb-string-value-converter")
public class StringValueConverter extends ValueConverter<String> {

  public StringValueConverter(){
    super();
  }

  public StringValueConverter(String key){
    super(key);
  }

  @Override
  String valueOf(Object o) {
    return String.valueOf(o);
  }
}
