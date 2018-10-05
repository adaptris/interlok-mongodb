package com.adaptris.core.mongodb;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.interlok.InterlokException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * Producer that updates JSON objects into MongoDB, if a JSON array is given the array will be split and inserted as individual JSON objects.
 *
 * Producer assumes you've formatted the json in required format:
 *
 * <p>
 *   Result:
 *   <pre>
 *     {@code
 *     [ { "$set" : { "stars" : "4" } } ]
 *     }
 *   </pre>
 * </p>
 *
 * @author mwarman
 * @config mongodb-update-producer
 */
@AdapterComponent
@ComponentProfile(summary = "Update JSON objects into MongoDB.", tag = "producer,mongodb",
    recommended = {MongoDBConnection.class})
@XStreamAlias("mongodb-update-producer")
public class MongoDBUpdateProducer extends MongoDBUpdateReplaceProducer {


  @Override
  UpdateResult actionDocument(MongoCollection<Document> collection, Bson filter, Document document) {
    return collection.updateOne(filter, document, new UpdateOptions().upsert(upsert()).bypassDocumentValidation(bypassDocumentValidation()));
  }

  public MongoDBUpdateProducer withFilterFields(List<String> filterFields) {
    setFilterFields(filterFields);
    return this;
  }

  public MongoDBUpdateProducer withUpsert(Boolean upsert){
    setUpsert(upsert);
    return this;
  }

  public MongoDBUpdateProducer withDestination(ProduceDestination destination){
    setDestination(destination);
    return this;
  }
}
