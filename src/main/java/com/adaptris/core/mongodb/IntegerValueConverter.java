package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config mongodb-integer-value-converter
 */
@XStreamAlias("mongodb-integer-value-converter")
public class IntegerValueConverter extends ValueConverter<Integer> {

  public IntegerValueConverter(){
    super();
  }

  public IntegerValueConverter(String key){
    super(key);
  }

  @Override
  Integer valueOf(Object o) {
    return Integer.valueOf(String.valueOf(o));
  }
}
