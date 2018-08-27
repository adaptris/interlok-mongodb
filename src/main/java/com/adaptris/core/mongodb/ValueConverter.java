package com.adaptris.core.mongodb;

import com.adaptris.core.util.Args;
import org.bson.Document;

public abstract class ValueConverter<T> {

  private String key;
  private String outputKey;

  public ValueConverter(){

  }

  public ValueConverter(String key){
    setKey(key);
  }

  public T convert(Document original){
    Object value = original.get(getKey());
    if (value == null){
      return null;
    }
    return valueOf(value);
  }

  abstract T valueOf(Object o);

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = Args.notEmpty(key, "key");
  }

  public String getOutputKey() {
    return outputKey;
  }

  public void setOutputKey(String outputKey) {
    this.outputKey = Args.notEmpty(outputKey, "outputKey");
  }

  String key(){
    return getOutputKey() != null ? getOutputKey() : getKey();
  }
}
