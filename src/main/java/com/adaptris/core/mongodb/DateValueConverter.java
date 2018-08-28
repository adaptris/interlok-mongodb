package com.adaptris.core.mongodb;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import java.text.SimpleDateFormat;
import java.util.Date;

@XStreamAlias("mongodb-date-value-converter")
public class DateValueConverter extends ValueConverter<Date> {

  @NotBlank
  private String dateFormat;

  private transient SimpleDateFormat dateFormatter = null;

  public DateValueConverter(){
    super();
  }

  public DateValueConverter(String key, String dateFormat){
    super(key);
    setDateFormat(dateFormat);
  }

  @Override
  Date valueOf(Object o) {
    try {
      if(StringUtils.isEmpty(String.valueOf(o))) {
        return null;
      } else {
        return getDateFormatter().parse(String.valueOf(o));
      }
    } catch (Exception e){
      throw new IllegalArgumentException(String.format("Failed to convert input string [%s] to type date", o), e);
    }
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  protected SimpleDateFormat getDateFormatter() {
    if(dateFormatter == null){
      dateFormatter = new SimpleDateFormat(getDateFormat());
    }
    return dateFormatter;
  }
}
