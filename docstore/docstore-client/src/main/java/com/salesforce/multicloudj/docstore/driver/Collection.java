package com.salesforce.multicloudj.docstore.driver;

import com.salesforce.multicloudj.docstore.client.Query;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

// A Collection is a set of documents.
// Refer to https://github.com/google/go-cloud. Some methods are dropped, including Conversion between Revision and byte[], and As(), ErrorAs(), ErrorCode().
public interface Collection {

    // Returns the document key with String type.
    Object getKey(Document document);

    // RevisionField returns the name of the field used to hold revisions.
    // If the empty string is returned, docstore.DefaultRevisionField will be used.
    String getRevisionField();

    // RunActions executes a slice of actions.
    // beforeDo is a callback which is called once before each action or action group.
    void runActions(List<Action> actions, Consumer<Predicate<Object>> beforeDo);

    // RunGetQuery executes a Query.
    DocumentIterator runGetQuery(Query query);

    // QueryPlan returns the plan for the query.
    String queryPlan(Query query);

    // Close cleans up any resources used by the Collection.
    void close();
}
