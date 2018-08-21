package com.adaptris.core.mongodb;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.interlok.InterlokException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * Producer that replaces JSON objects into MongoDB, if a JSON array is given the array will be split and inserted as individual JSON objects.
 *
 * @author mwarman
 * @config mongodb-replace-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Replace JSON objects into MongoDB.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@XStreamAlias("mongodb-replace-producer")
public class MongoDBReplaceProducer extends MongoDBArrayProducer {

  @XStreamImplicit(itemFieldName = "filter-fields")
  private List<String> filterFields = new ArrayList<>();

  private Boolean upsert;

  @Override
  public void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message) throws InterlokException{
    Document document = Document.parse(message.getContent());
    List<Bson> filters = new ArrayList<>();
    for(String field : getFilterFields()) {
      filters.add(Filters.eq(field, document.get(field)));
    }
    Bson filter = Filters.and(filters);
    collection.replaceOne(filter, document, new ReplaceOptions().upsert(upsert()));
  }

  public List<String> getFilterFields() {
    return filterFields;
  }

  public void setFilterFields(List<String> filterFields) {
    this.filterFields = filterFields;
  }

  public Boolean getUpsert() {
    return upsert;
  }

  public void setUpsert(Boolean upsert) {
    this.upsert = upsert;
  }

  private boolean upsert(){
    return getUpsert() != null ? getUpsert() : false;
  }

  public MongoDBReplaceProducer withFilterFields(List<String> filterFields) {
    setFilterFields(filterFields);
    return this;
  }

  public MongoDBReplaceProducer withUpsert(Boolean upsert){
    setUpsert(upsert);
    return this;
  }

  public MongoDBReplaceProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }
}
