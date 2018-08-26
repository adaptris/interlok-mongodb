package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("mongodb-double-value-converter")
public class DoubleValueConverter extends ValueConverter<Double> {

  public DoubleValueConverter(){
    super();
  }

  public DoubleValueConverter(String key){
    super(key);
  }

  @Override
  Double valueOf(Object o) {
    return Double.valueOf((String)o);
  }
}
