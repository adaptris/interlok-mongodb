package com.adaptris.core.mongodb;

import java.util.ArrayList;
import java.util.List;

import jakarta.validation.Valid;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import lombok.NoArgsConstructor;

/**
 * Producer that replaces JSON objects into MongoDB, if a JSON array is given the array will be split and inserted as individual JSON
 * objects.
 *
 * @author mwarman
 * @config mongodb-replace-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Replace JSON objects into MongoDB.", tag = "producer,mongodb", recommended = { MongoDBConnection.class })
@XStreamAlias("mongodb-replace-producer")
@DisplayOrder(order = { "collection" })
@NoArgsConstructor
public class MongoDBReplaceProducer extends MongoDBUpdateReplaceProducer {

  @Valid
  @XStreamImplicit
  private List<ValueConverter<?>> valueConverters = new ArrayList<>();

  @Override
  UpdateResult actionDocument(MongoCollection<Document> collection, Bson filter, Document document) {
    for (ValueConverter<?> valueConverter : getValueConverters()) {
      document.put(valueConverter.key(), valueConverter.convert(document));
    }
    return collection.replaceOne(filter, document,
        new ReplaceOptions().upsert(upsert()).bypassDocumentValidation(bypassDocumentValidation()));
  }

  public List<ValueConverter<?>> getValueConverters() {
    return valueConverters;
  }

  public void setValueConverters(List<ValueConverter<?>> valueConverters) {
    this.valueConverters = valueConverters;
  }

  public MongoDBReplaceProducer withFilterFields(List<String> filterFields) {
    setFilterFields(filterFields);
    return this;
  }

  public MongoDBReplaceProducer withUpsert(Boolean upsert) {
    setUpsert(upsert);
    return this;
  }

}
