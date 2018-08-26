package com.adaptris.core.mongodb;

import com.adaptris.core.util.Args;
import org.bson.Document;

public abstract class ValueConverter<T> {

  private String key;

  public ValueConverter(){

  }

  public ValueConverter(String key){
    setKey(key);
  }

  public T convert(Document original){
    return valueOf(original.get(getKey()));
  }

  abstract T valueOf(Object o);

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = Args.notEmpty(key, "key");
  }
}
