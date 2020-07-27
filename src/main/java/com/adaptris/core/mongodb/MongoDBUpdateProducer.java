package com.adaptris.core.mongodb;

import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

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
@DisplayOrder(order = {"collection"})
@NoArgsConstructor
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

}
