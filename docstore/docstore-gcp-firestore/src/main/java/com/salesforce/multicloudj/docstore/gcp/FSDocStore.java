package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.Write;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Util;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Google Cloud Firestore implementation of the AbstractDocStore.
 * <p>
 * This implementation provides document operations for Google Cloud Firestore using
 * the low-level Firestore V1 API.
 * Usage example:
 * <pre>
 * {@code
 * // Create a Firestore DocStore with builder
 * FSDocStore store = new FSDocStore.Builder()
 *     .withCollectionOptions(new CollectionOptions.CollectionOptionsBuilder()
 *         .withTableName("my-collection")
 *         .withPartitionKey("id")
 *         .build())
 *     .build();
 *
 * }
 * </pre>
 */
@AutoService(AbstractDocStore.class)
public class FSDocStore extends AbstractDocStore {

    /** The low-level Firestore client used for operations */
    private FirestoreClient firestoreClient;

    /**
     * Default constructor that initializes with a default builder.
     * Used primarily for service discovery.
     */
    public FSDocStore() {
        super(new Builder());
    }

    /**
     * Constructor that initializes with the provided builder configuration.
     *
     * @param builder The builder containing configuration for this store
     */
    public FSDocStore(Builder builder) {
        super(builder);
        this.firestoreClient = builder.firestoreClient;
    }

    /**
     * Internal key class representing a Firestore document identifier.
     * <p>
     * In Firestore, documents are identified by their document ID within a collection.
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    private static class Key {
        /** The document ID in Firestore */
        private String documentId;

        @Override
        public String toString() {
            return "documentId:" + documentId;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a new builder for the FSDocStore.
     */
    @Override
    public Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Maps Firestore-specific exceptions to Substrate SDK exceptions.
     * <p>
     * This method handles:
     * <ul>
     *   <li>Google API exceptions (ApiException) based on their status codes</li>
     *   <li>IllegalArgumentException as InvalidArgumentException</li>
     *   <li>Other exceptions as UnknownException</li>
     * </ul>
     *
     * @param t The exception to map
     * @return The appropriate SubstrateSdkException class
     */
    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // Check if exception is already an SDK exception
        if (t instanceof SubstrateSdkException) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }

        // Check full exception chain for known exception types
        Set<Throwable> exceptions = ExceptionUtils.getThrowableList(t).stream().limit(5).collect(Collectors.toSet());

        // Handle client-side validation exceptions
        if (exceptions.stream().anyMatch(IllegalArgumentException.class::isInstance)) {
            return InvalidArgumentException.class;
        }

        // Handle GAX/API exceptions
        ApiException apiException = exceptions.stream()
                .filter(ApiException.class::isInstance)
                .map(ApiException.class::cast)
                .findFirst()
                .orElse(null);

        if (apiException != null) {
            StatusCode.Code code = apiException.getStatusCode().getCode();
            return ErrorCodeMapping.getException(code);
        }

        // Default fallback
        return UnknownException.class;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Gets the key for a document using the partition key field defined in the collection options.
     * <p>
     * In Firestore, the document ID is the primary key, which is extracted from the partition key field.
     *
     * @param document The document to get the key from
     * @return A Key object containing the document ID
     * @throws IllegalArgumentException if the partition key is missing or empty
     */
    @Override
    public Key getKey(Document document) {
        // In Firestore, the document ID is the primary key.
        // Assuming the partition key field holds the document ID.
        String docId = (String) document.getField(collectionOptions.getPartitionKey());
        if (docId == null || docId.isEmpty()) {
            throw new IllegalArgumentException("Document ID (partitionKey) cannot be null or empty for Firestore");
        }
        return new Key(docId);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Executes a list of actions against Firestore.
     * <p>
     * This method:
     * <ol>
     *   <li>Groups actions by type (GET, PUT, etc.)</li>
     *   <li>Executes GETs before writes to ensure data consistency</li>
     *   <li>Performs writes in batches when possible</li>
     *   <li>Handles atomic writes through Firestore transactions when required</li>
     * </ol>
     *
     * @param actions List of actions to execute
     * @param beforeDo Optional callback executed before each action
     * @throws SubstrateSdkException If an error occurs during execution
     */
    @Override
    public void runActions(List<Action> actions, Consumer<Predicate<Object>> beforeDo) {
        // Firestore batch writes have a limit (e.g., 500 operations).
        // We might need to split large action lists.

        // Separate action lists (similar to AWS version, but maybe simpler for Firestore initially)
        List<Action> beforeGets = new ArrayList<>();
        List<Action> getList = new ArrayList<>();
        List<Action> writeList = new ArrayList<>();
        List<Action> atomicWriteList = new ArrayList<>(); // Firestore transactions handle atomicity differently
        List<Action> afterGets = new ArrayList<>();

        Util.groupActions(actions, beforeGets, getList, writeList, atomicWriteList, afterGets);

        try {
            // Run gets first (can be parallelized if needed)
            runGets(beforeGets, beforeDo);
            runGets(getList, beforeDo);

            // Process non-atomic writes in batches
            runWritesBatched(writeList, beforeDo);

            // Process atomic writes (transactions) - Requires different implementation
            runAtomicWrites(atomicWriteList, beforeDo);

            // Run after gets
            runGets(afterGets, beforeDo);

        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() != null) {
                throw new SubstrateSdkException("Error executing Firestore actions", e.getCause());
            }
            throw new SubstrateSdkException("Error executing Firestore actions", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for Firestore actions to complete.", e);
        }
    }

    /**
     * Processes GET operations.
     * <p>
     * Retrieves document data from Firestore based on the actions provided.
     *
     * @param gets List of GET actions to process
     * @param beforeDo Optional callback executed before each GET
     * @throws ExecutionException If an execution error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    private void runGets(List<Action> gets, Consumer<Predicate<Object>> beforeDo) throws ExecutionException, InterruptedException {
        // Implementation will be added for batch gets using the low-level API
    }

    /**
     * Processes non-atomic write operations in a single batch.
     * <p>
     * Batches compatible write operations together for improved performance.
     *
     * @param writes List of write actions to process
     * @param beforeDo Optional callback executed before each write
     * @throws ExecutionException If an execution error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    private void runWritesBatched(List<Action> writes, Consumer<Predicate<Object>> beforeDo) throws ExecutionException, InterruptedException {
        if (writes.isEmpty()) {
            return;
        }

        // Create a list for all write operations
        List<Write> allWrites = new ArrayList<>();

        // Process each action and create write operations
        for (Action action : writes) {
            // Skip non-PUT actions for now
            if (action.getKind() != ActionKind.ACTION_KIND_PUT) {
                continue;
            }

            Key key = getKey(action.getDocument());

            // Execute beforeDo callback for this action
            if (beforeDo != null) {
                beforeDo.accept(k -> k.equals(key));
            }

            // Get the full document path
            String documentPath = getDocumentPath(key.getDocumentId());

            // Create the write operation
            Write write = createPutWrite(action.getDocument(), documentPath);

            // Add to our list of writes
            allWrites.add(write);
        }

        // If we have writes to process, send them in a single batch
        if (!allWrites.isEmpty()) {
            // Create a single CommitRequest with all writes
            CommitRequest commitRequest = CommitRequest.newBuilder()
                    .setDatabase(getDatabasePath())
                    .addAllWrites(allWrites)
                    .build();

            // Send the commit request with all writes
            CommitResponse response = firestoreClient.commit(commitRequest);
            System.out.println(response);
        }
    }

    /**
     * Gets the full document path for a document ID, handling both simple collection names
     * and full collection paths.
     *
     * @param documentId The document ID
     * @return The full document path
     */
    private String getDocumentPath(String documentId) {
        String tableName = collectionOptions.getTableName();

        // Check if tableName is already a full path
        if (tableName.startsWith("projects/")) {
            // It's already a full path, just append the document ID
            return tableName + "/" + documentId;
        } else {
            // It's a simple collection name, build the full path
            return getDatabasePath() + "/documents/" + tableName + "/" + documentId;
        }
    }

    /**
     * Gets the database path from the collection name or uses the environment's default.
     * <p>
     * Extracts the database path using regex if the collection name is a full path.
     *
     * @return The Firestore database path
     */
    private String getDatabasePath() {
        String tableName = collectionOptions.getTableName();
        Pattern pattern = Pattern.compile("^(projects/[^/]+/databases/[^/]+)/documents/.+");
        Matcher matcher = pattern.matcher(tableName);
        // Attempt to match the entire string
        if (matcher.matches()) {
            // group(1) is "projects/.../databases/(default)"
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("Collection name does not match the expected Firestore path pattern");
        }
    }

    /**
     * Creates a Write operation for a PUT action.
     *
     * @param doc The document to write
     * @param documentPath Full path to the document
     * @return A Write operation
     */
    private Write createPutWrite(Document doc, String documentPath) {
        // Build the document with fields from FSCodec
        com.google.firestore.v1.Document.Builder docBuilder = com.google.firestore.v1.Document.newBuilder();
        docBuilder.setName(documentPath);
        docBuilder.putAllFields(FSCodec.encodeDoc(doc));

        // Create a write operation with the document
        return Write.newBuilder()
                .setUpdate(docBuilder.build())
                .build();
    }

    /**
     * Processes atomic writes using Firestore transactions.
     * <p>
     * Ensures that a set of operations either all succeed or all fail.
     *
     * @param writes List of atomic write actions
     * @param beforeDo Optional callback executed before each write
     * @throws ExecutionException If an execution error occurs
     * @throws InterruptedException If the operation is interrupted
     */
    private void runAtomicWrites(List<Action> writes, Consumer<Predicate<Object>> beforeDo) throws ExecutionException, InterruptedException {
        // Implementation for atomic writes using the low-level API
    }

    /**
     * {@inheritDoc}
     * <p>
     * Executes a query against Firestore and returns an iterator over the results.
     * <p>
     * Note: This feature is currently not fully implemented.
     *
     * @param query The query to execute
     * @return A DocumentIterator for the query results
     * @throws UnsupportedOperationException Currently thrown as this feature is not implemented
     */
    @Override
    public DocumentIterator runGetQuery(Query query) {
        return new DocumentIterator() {
            @Override
            public void next(Document document) {

            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public void stop() {

            }
        };
    }

    /**
     * {@inheritDoc}
     * <p>
     * Generates a query execution plan for the given query.
     * <p>
     * Note: This feature is currently not fully implemented.
     *
     * @param query The query to plan
     * @return A string representation of the query plan
     */
    @Override
    public String queryPlan(Query query) {
        return "Firestore query plan not yet implemented.";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes resources and connections associated with this store.
     * <p>
     * This method closes the Firestore client if it was created.
     *
     * @throws UnknownException If an error occurs while closing resources
     */
    @Override
    public void close() {
        try {
            if (firestoreClient != null) {
                firestoreClient.close();
            }
        } catch (Exception e) {
            throw new UnknownException("Unable to close the connection", e);
        }
    }

    /**
     * Builder for creating FSDocStore instances.
     * <p>
     * This builder allows for configuration of:
     * <ul>
     *   <li>Firestore client settings</li>
     *   <li>Authentication credentials</li>
     *   <li>Collection configuration</li>
     * </ul>
     */
    public static class Builder extends AbstractDocStore.Builder<FSDocStore, Builder> {
        /** The Firestore client to use for operations */
        private FirestoreClient firestoreClient;

        /** Google credentials for authentication */
        private GoogleCredentials credentials;

        /**
         * Default constructor that sets the provider ID.
         */
        public Builder() {
            providerId("gcp-firestore");
        }

        /**
         * Sets the low-level Firestore V1 client.
         *
         * @param firestoreClient The Firestore client to use
         * @return This builder for chaining
         */
        public Builder withFirestoreV1Client(FirestoreClient firestoreClient) {
            this.firestoreClient = firestoreClient;
            return self();
        }

        /**
         * Builds a Firestore V1 client from the current configuration.
         *
         * @return A configured FirestoreClient
         * @throws SubstrateSdkException If client creation fails
         */
        private FirestoreClient buildFirestoreV1Client() throws SubstrateSdkException {
            try {
                // Create FirestoreSettings with credentials
                FirestoreSettings.Builder settingsBuilder = FirestoreSettings.newBuilder();

                if (credentials != null) {
                    settingsBuilder.setCredentialsProvider(() -> credentials);
                }

                // Build the client
                return FirestoreClient.create(settingsBuilder.build());
            } catch (Exception e) {
                throw new SubstrateSdkException("Failed to build Firestore V1 client", e);
            }
        }

        @Override
        public Builder self() {
            return this;
        }

        /**
         * Builds an FSDocStore instance with the current configuration.
         *
         * @return A new FSDocStore instance
         * @throws SubstrateSdkException If store creation fails
         */
        @Override
        public FSDocStore build() {
            if (this.firestoreClient == null) {
                this.firestoreClient = buildFirestoreV1Client();
            }

            return new FSDocStore(this);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Performs batch GET operations for multiple documents.
     *
     * @param gets List of GET actions
     * @param beforeDo Optional callback executed before each GET
     * @param minGets Minimum number of documents to retrieve in a batch
     * @param maxGets Maximum number of documents to retrieve in a batch
     */
    @Override
    public void batchGet(List<Action> gets, Consumer<Predicate<Object>> beforeDo, int minGets, int maxGets) {
        // Implementation for batch get using the low-level API
    }
}

