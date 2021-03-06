package com.adaptris.core.mongodb;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config mongodb-milliseconds-value-converter
 */
@XStreamAlias("mongodb-milliseconds-value-converter")
public class MillisecondsValueConverter extends ValueConverter<Long> {

  private String dateFormat;

  private transient SimpleDateFormat dateFormatter = null;

  public MillisecondsValueConverter(){
    super();
  }

  public MillisecondsValueConverter(String key, String dateFormat){
    super(key);
    setDateFormat(dateFormat);
  }

  @Override
  Long valueOf(Object o) {
    try {
      if(o instanceof Date){
        return ((Date) o).getTime();
      } else {
        if(StringUtils.isEmpty(String.valueOf(o))) {
          return 0L;
        } else {
          return getDateFormatter().parse(String.valueOf(o)).getTime();
        }
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
