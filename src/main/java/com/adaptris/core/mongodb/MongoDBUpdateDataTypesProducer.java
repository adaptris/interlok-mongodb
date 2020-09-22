package com.adaptris.core.mongodb;

import static com.mongodb.client.model.Filters.eq;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import javax.validation.Valid;
import org.bson.Document;
import org.bson.types.ObjectId;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.UpdateResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import lombok.NoArgsConstructor;


/**
 *
 * @author mwarman
 * @config mongodb-update-data-types-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Update data types in MongoDB.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@XStreamAlias("mongodb-update-data-types-producer")
@NoArgsConstructor
@DisplayOrder(order = {"collection"})
public class MongoDBUpdateDataTypesProducer extends MongoDBProducer {

  @Valid
  private DataInputParameter<String> filter;

  @Valid
  @XStreamImplicit
  private List<ValueConverter> valueConverters = new ArrayList<>();

  @AdvancedConfig
  private Integer batchSize;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String collectionName, long timeout,
      AdaptrisMessage reply) throws ProduceException {
    try {
      MongoCollection<Document> findCollection = getMongoDatabase().getCollection(collectionName);
      MongoCollection<Document> updateCollection = getMongoDatabase().getCollection(collectionName);
      MongoIterable<Document> iterable =  findCollection.find(createFilter(msg));
      if(getBatchSize() != null) {
        iterable.batchSize(getBatchSize());
      }
      for(Document original : iterable){
        ObjectId id = original.getObjectId("_id");
        LinkedHashMap<String, Object> updates = new LinkedHashMap<>();
        for(ValueConverter valueConverter : getValueConverters()){
          updates.put(valueConverter.key(), valueConverter.convert(original));
        }
        Document result = new Document();
        result.put("$set", updates);
        UpdateResult updateResult = updateCollection.updateOne(eq("_id", id), result);
        log.trace(updateResult.toString());
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private Document createFilter(AdaptrisMessage message) throws InterlokException {
    return getFilter() != null ? Document.parse(getFilter().extract(message)) : new Document();
  }

  public DataInputParameter<String> getFilter() {
    return filter;
  }

  public void setFilter(DataInputParameter<String> filter) {
    this.filter = filter;
  }

  public List<ValueConverter> getValueConverters() {
    return valueConverters;
  }

  public void setValueConverters(List<ValueConverter> valueConverters) {
    this.valueConverters = valueConverters;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public MongoDBUpdateDataTypesProducer withFilter(DataInputParameter<String> filter) {
    setFilter(filter);
    return this;
  }

  public MongoDBUpdateDataTypesProducer withTypeConverters(List<ValueConverter> valueConverters) {
    setValueConverters(valueConverters);
    return this;
  }
}
