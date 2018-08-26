package com.adaptris.core.mongodb;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.interlok.InterlokException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mwarman
 */
public abstract class MongoDBUpdateReplaceProducer extends MongoDBArrayProducer {

  @XStreamImplicit(itemFieldName = "filter-field")
  private List<String> filterFields = new ArrayList<>();

  private Boolean upsert;

  private Boolean bypassDocumentValidation;

  @Override
  public final void parseAndActionDocument(MongoCollection<Document> collection, AdaptrisMessage message) throws InterlokException{
    Document document = Document.parse(message.getContent());
    List<Bson> filters = new ArrayList<>();
    for(String field : getFilterFields()) {
      filters.add(Filters.eq(field, document.get(field)));
    }
    Bson filter = Filters.and(filters);
    actionDocument(collection, filter, document);
  }

  abstract void actionDocument(MongoCollection<Document> collection, Bson filter, Document document);

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

  boolean upsert(){
    return getUpsert() != null ? getUpsert() : false;
  }

  public Boolean getBypassDocumentValidation() {
    return bypassDocumentValidation;
  }

  public void setBypassDocumentValidation(Boolean bypassDocumentValidation) {
    this.bypassDocumentValidation = bypassDocumentValidation;
  }

  boolean bypassDocumentValidation(){
    return getBypassDocumentValidation() != null ? getBypassDocumentValidation() : false;
  }
}
