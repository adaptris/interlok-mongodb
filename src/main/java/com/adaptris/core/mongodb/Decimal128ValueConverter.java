package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.bson.types.Decimal128;

@XStreamAlias("mongodb-decimal-128-value-converter")
public class Decimal128ValueConverter extends ValueConverter<Decimal128> {

  public Decimal128ValueConverter(){
    super();
  }

  public Decimal128ValueConverter(String key){
    super(key);
  }

  @Override
  Decimal128 valueOf(Object o) {
    return Decimal128.parse((String)o);
  }
}
