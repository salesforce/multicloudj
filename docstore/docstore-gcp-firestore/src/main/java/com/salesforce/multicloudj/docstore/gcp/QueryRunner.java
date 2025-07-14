package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * QueryRunner for Firestore that creates streaming query clients.
 */
@AllArgsConstructor
@Getter
public class QueryRunner {

    /**
     * The Firestore client used for running queries
     */
    private FirestoreClient firestoreClient;

    /**
     * The RunQueryRequest to execute
     */
    private RunQueryRequest queryRequest;

    /**
     * Callback to execute before running a query
     */
    private Consumer<Predicate<Object>> beforeRun;

    /**
     * Get the string representation of this query's execution plan
     * 
     * @return A string description of the query plan
     */
    public String queryPlan() {
        return "";
    }

    /**
     * Create and return a server stream for proper lifecycle management
     * 
     * @return A ServerStream that can be properly closed/cancelled
     */
    public ServerStream<RunQueryResponse> createServerStream() {
        // Return the server stream directly for proper lifecycle management
        return firestoreClient.runQueryCallable().call(queryRequest);
    }
} 