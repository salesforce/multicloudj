package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ServerStream;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Value;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator for Firestore query results.
 * This implementation properly manages the gRPC stream lifecycle to prevent thread leaks.
 */
public class FSDocumentIterator implements DocumentIterator {

    /**
     * The underlying gRPC server stream
     */
    private ServerStream<RunQueryResponse> serverStream;

    /**
     * Iterator for query responses from the streaming client
     */
    private Iterator<RunQueryResponse> responseIterator;

    /**
     * Number of documents to skip (offset)
     */
    private int offset = 0;

    /**
     * Maximum number of documents to return (limit)
     */
    private int limit = 0;

    /**
     * Flag indicating if we've reached the end
     */
    private boolean done = false;

    /**
     * The next response that's ready to be consumed (look-ahead)
     */
    private RunQueryResponse nextResponse = null;

    /**
     * Flag indicating if we've looked ahead and found the next response
     */
    private boolean hasLookedAhead = false;

    /**
     * Pagination token for tracking the last document
     */
    private FSPaginationToken paginationToken = null;

    /**
     * Reference to the FSDocStore for accessing getKey method
     */
    private FSDocStore docStore;

    /**
     * The query containing ORDER BY information
     */
    private Query query;

    /**
     * The last document that was processed (for pagination token)
     */
    private Document lastDocument = null;

    /**
     * Creates a new iterator for Firestore documents.
     *
     * @param queryRunner The query runner to use
     * @param query The query containing offset, limit, and ORDER BY information
     * @param docStore Reference to the FSDocStore for accessing getKey method
     */
    public FSDocumentIterator(QueryRunner queryRunner, Query query, FSDocStore docStore) {
        this.query = query;
        this.offset = query.getOffset();
        this.limit = query.getLimit();
        this.docStore = docStore;
        this.paginationToken = (FSPaginationToken) query.getPaginationToken();
        
        // Get the server stream and iterator
        this.serverStream = queryRunner.createServerStream();
        this.responseIterator = this.serverStream.iterator();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Gets the next document from the stream and populates the provided document.
     *
     * @param document The document to populate with data
     * @throws NoSuchElementException if there are no more documents
     */
    @Override
    public void next(Document document) {
        if (!hasNext()) {
            throw new NoSuchElementException("No more documents in the iterator");
        }

        // Use the looked-ahead response or get a new one
        RunQueryResponse response;
        if (hasLookedAhead && nextResponse != null) {
            response = nextResponse;
            nextResponse = null;
            hasLookedAhead = false;
        } else {
            response = getNextResponseInternal();
        }

        if (response == null || !response.hasDocument()) {
            markDoneAndClose();
            throw new NoSuchElementException("No more documents available");
        }

        // Decode the document
        FSCodec.decodeDoc(response.getDocument(), document, null);

        // Store this document as the last document for potential pagination token update
        // We'll only update the pagination token when we're actually done
        lastDocument = document;

        // Initialize pagination token if needed
        if (paginationToken == null) {
            paginationToken = new FSPaginationToken();
        }
    }

    /**
     * Updates the pagination token with cursor values from the last document.
     * This should only be called when we're done iterating or at the limit.
     * The pagination cursor should always include the field of order by clause
     * in order for it work correctly.
     * 
     * @param document The last document to extract cursor values from
     */
    private void updatePaginationToken(Document document) {
        if (document == null) {
            return;
        }

        List<Value> cursorValues = new ArrayList<>();
        
        // Check if we have an explicit ORDER BY field from the query
        String orderByField = query.getOrderByField();
        
        if (orderByField != null && !orderByField.isEmpty()) {
            // If there's an explicit ORDER BY field, include both the field value and document key
            Object fieldValue = document.getField(orderByField);
            if (fieldValue != null) {
                cursorValues.add(FSCodec.encodeValue(fieldValue));
            }
        }
        
        // Always include the document key for unique pagination
        // Extract the document ID using the getKey() method
        Object key = docStore.getKey(document);
        // The key.toString() method returns "documentId:actualId" format
        String keyString = key.toString();
        // Extract just the document ID part after "documentId:"
        String docId = keyString.substring(keyString.indexOf(":") + 1);
        
        // Build the full document path for the reference
        String fullDocumentPath = String.format("%s/documents/%s/%s",
                docStore.getDatabasePath(), docStore.getCollectionName(), docId);
        
        // Create a ReferenceValue for the document key
        Value referenceValue = Value.newBuilder()
            .setReferenceValue(fullDocumentPath)
            .build();
        cursorValues.add(referenceValue);
        
        paginationToken.setCursorValues(cursorValues);
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Checks if there are more documents available by looking ahead in the stream.
     * 
     * @return true if there are more documents, false otherwise
     */
    @Override
    public boolean hasNext() {
        if (done) {
            return false;
        }

        // If we haven't looked ahead yet, do it now
        if (!hasLookedAhead) {
            // Look ahead to see if there's a next document
            nextResponse = getNextResponseInternal();
            hasLookedAhead = true;

            if (nextResponse == null || !nextResponse.hasDocument()) {
                // Update pagination token with the last document before marking as done
                if (lastDocument != null) {
                    updatePaginationToken(lastDocument);
                }
                markDoneAndClose();
                return false;
            }
        }

        return nextResponse != null && nextResponse.hasDocument();
    }

    /**
     * Gets the next response from the stream, handling empty responses.
     * 
     * @return The next response with a document, or null if stream is exhausted
     */
    private RunQueryResponse getNextResponseInternal() {
        try {
            while (responseIterator.hasNext()) {
                RunQueryResponse response = responseIterator.next();
                
                // Skip responses without documents (partial progress responses)
                if (response.hasDocument()) {
                    return response;
                }
                // Continue looping for responses without documents
            }
        } catch (Exception e) {
            // If there's any error reading from the stream, mark as done and close
            markDoneAndClose();
        }
        
        // Stream is exhausted
        return null;
    }

    /**
     * Marks the iterator as done and properly closes the underlying stream
     */
    private void markDoneAndClose() {
        if (!done) {
            done = true;
            closeStream();
        }
    }

    /**
     * Properly closes the underlying gRPC stream to free up threads
     */
    private void closeStream() {
        try {
            if (serverStream != null) {
                // Cancel the server stream to free up resources
                serverStream.cancel();
                serverStream = null;
            }
        } catch (Exception e) {
            // Ignore errors during cleanup, but ensure we don't leak resources
        }
        responseIterator = null;
        nextResponse = null;
        hasLookedAhead = false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Stops iteration and releases resources.
     */
    @Override
    public void stop() {
        markDoneAndClose();
    }

    @Override
    public PaginationToken getPaginationToken() {
        return paginationToken;
    }
} 