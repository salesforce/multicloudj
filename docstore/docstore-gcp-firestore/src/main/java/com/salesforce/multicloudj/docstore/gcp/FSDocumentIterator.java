package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ServerStream;
import com.google.firestore.v1.RunQueryResponse;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;

import java.util.Iterator;
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
     * Number of documents processed so far
     */
    private int documentsProcessed = 0;

    /**
     * Number of documents returned so far
     */
    private int documentsReturned = 0;

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
     * Creates a new iterator for Firestore documents.
     *
     * @param queryRunner The query runner to use
     * @param offset Number of documents to skip
     * @param limit Maximum number of documents to return
     */
    public FSDocumentIterator(QueryRunner queryRunner, int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
        
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

        // Skip documents until we reach the offset
        while (documentsProcessed < offset) {
            RunQueryResponse response = getNextResponseInternal();
            if (response != null && response.hasDocument()) {
                documentsProcessed++;
            }
        }

        // Check limit
        if (limit > 0 && documentsReturned >= limit) {
            markDoneAndClose();
            throw new NoSuchElementException("Limit reached");
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
        documentsProcessed++;
        documentsReturned++;
        
        // If we've reached the limit, close the stream
        if (limit > 0 && documentsReturned >= limit) {
            markDoneAndClose();
        }
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

        // Check limit first
        if (limit > 0 && documentsReturned >= limit) {
            markDoneAndClose();
            return false;
        }

        // If we haven't looked ahead yet, do it now
        if (!hasLookedAhead) {
            // Skip documents until we reach the offset
            while (documentsProcessed < offset) {
                RunQueryResponse response = getNextResponseInternal();
                if (response == null) {
                    markDoneAndClose();
                    return false;
                }
                if (response.hasDocument()) {
                    documentsProcessed++;
                }
            }

            // Look ahead to see if there's a next document
            nextResponse = getNextResponseInternal();
            hasLookedAhead = true;

            if (nextResponse == null || !nextResponse.hasDocument()) {
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
        return null;
    }
} 