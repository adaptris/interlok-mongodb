package com.adaptris.core.mongodb;

import java.lang.reflect.ParameterizedType;
import jakarta.validation.constraints.NotBlank;
import org.bson.Document;
import com.adaptris.core.util.Args;

public abstract class ValueConverter<T> {

  @NotBlank
  private String key;
  private String outputKey;

  public ValueConverter(){

  }

  public ValueConverter(String key){
    setKey(key);
  }

  @SuppressWarnings("unchecked")
  public T convert(Document original){
    String[] keys = getKey().split("\\.");
    Document document = original;
    Object value = null;
    for (String key: keys) {
      value = document.get(key);
      if(value instanceof Document){
        document = (Document)value;
      }
    }
    if (value == null){
      return null;
    }
    Class<T> type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];;
    if(type.isInstance(value)) {
      return (T)value;
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
