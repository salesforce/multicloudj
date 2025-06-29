package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.DocumentMask;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final int batchSize = 100;
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
        // To simulate PK + SK pattern, we can concat them together to form
        // a single document ID.
        String docId = (String) document.getField(collectionOptions.getPartitionKey());
        if (docId == null || docId.isEmpty()) {
            throw new IllegalArgumentException("Document ID (partitionKey) cannot be null or empty for Firestore");
        }
        if (collectionOptions.getSortKey() != null && document.getField(collectionOptions.getSortKey()) != null) {
            docId += ":" + document.getField(collectionOptions.getSortKey());
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
            runGets(beforeGets, beforeDo, batchSize);
            runGets(getList, beforeDo, batchSize);

            // Process non-atomic writes in batches
            runWritesBatched(writeList, beforeDo);

            // Process atomic writes (transactions) - Requires different implementation
            runAtomicWrites(atomicWriteList, beforeDo);

            // Run after gets
            runGets(afterGets, beforeDo, batchSize);

        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() != null) {
                throw new SubstrateSdkException("Error executing Firestore get actions", e.getCause());
            }
            throw new SubstrateSdkException("Error executing Firestore get actions", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for Firestore get actions to complete.", e);
        }
    }

    /**
     * Processes non-atomic write operations in batches.
     * <p>
     * Batches compatible write operations together for improved performance.
     * Unlike atomic writes, these may be split into multiple commits for optimization.
     * Each individual batch is atomic, but there's no atomicity guarantee across batches.
     *
     * @param writes List of write actions to process
     * @param beforeDo Optional callback executed before each write
     */
    private void runWritesBatched(List<Action> writes, Consumer<Predicate<Object>> beforeDo) {
        if (writes.isEmpty()) {
            return;
        }

        // Create a list for all write operations
        List<Write> allWrites = new ArrayList<>();
        // Map to keep track of which action corresponds to which write
        Map<Integer, Action> writeToActionMap = new HashMap<>();
        int writeIndex = 0;

        // Process each action and create write operations
        for (Action action : writes) {
            // Handle all write action types: PUT, REPLACE, CREATE, DELETE, UPDATE
            if (action.getKind() != ActionKind.ACTION_KIND_PUT && 
                action.getKind() != ActionKind.ACTION_KIND_REPLACE &&
                action.getKind() != ActionKind.ACTION_KIND_CREATE &&
                action.getKind() != ActionKind.ACTION_KIND_DELETE &&
                action.getKind() != ActionKind.ACTION_KIND_UPDATE) {
                continue;
            }

            Key key = getKey(action.getDocument());

            // Execute beforeDo callback for this action
            if (beforeDo != null) {
                beforeDo.accept(k -> k.equals(key));
            }

            // Get the full document path
            String documentPath = getDocumentPath(key.getDocumentId());

            // Create the appropriate write operation based on action kind
            Write write;
            if (action.getKind() == ActionKind.ACTION_KIND_DELETE) {
                write = createDeleteWrite(action.getDocument(), documentPath, action.getKind());
            } else if (action.getKind() == ActionKind.ACTION_KIND_UPDATE) {
                write = createUpdateWrite(action.getDocument(), documentPath, action.getMods());
            } else {
                write = createPutWrite(action.getDocument(), documentPath, action.getKind());
            }

            allWrites.add(write);
            writeToActionMap.put(writeIndex++, action);
        }

        // If we have writes to process, send them in a single batch
        if (!allWrites.isEmpty()) {
            // Create a single CommitRequest with all writes
            CommitRequest commitRequest = CommitRequest.newBuilder()
                    .setDatabase(getDatabasePath())
                    .addAllWrites(allWrites)
                    .build();

            // Send the commit request with all writes
            CommitResponse response = null;
            try {
                response = firestoreClient.commit(commitRequest);
            }  catch (ApiException e) {
                // When a precondition fails, the error handling depends on the action type.
                // Because we batch writes, we inspect only the first action's kind to decide
                // which exception to throw. This is always correct for a single-Do action.
                // For multi-action batches, however, we can't guarantee throwing the exact
                // exception for each individual action.
                // It's a weird thing that firestore throws two different errors for precondition
                // failures. For create with exists precondition, it aborts the transaction with Conflict.
                if (e.getCause().getMessage().contains("FAILED_PRECONDITION")
                || (e instanceof AbortedException && e.getMessage().equals("Conflict")) ) {
                    if (writes.get(0).getKind() == ActionKind.ACTION_KIND_CREATE) {
                        throw new ResourceAlreadyExistsException(e);
                    } else {
                        throw new ResourceNotFoundException(e);
                    }
                }
                throw e;
            }

            // Set the revision field in documents using the update time from the response
            for (int i = 0; i < Objects.requireNonNull(response).getWriteResultsCount(); i++) {
                Action action = writeToActionMap.get(i);
                if (action != null && action.getDocument().hasField(getRevisionField())) {
                    Timestamp updateTime = response.getWriteResults(i).getUpdateTime();
                    action.getDocument().setField(getRevisionField(), updateTime);
                }
            }
        }
    }

    /**
     * Creates a Write operation for a DELETE action.
     *
     * @param doc The document to delete
     * @param documentPath Full path to the document
     * @param actionKind The type of action being performed
     * @return A Write operation for deletion
     */
    private Write createDeleteWrite(Document doc, String documentPath, ActionKind actionKind) {
        // Create a delete operation
        Write.Builder writeBuilder = Write.newBuilder()
                .setDelete(documentPath);

        // Add precondition based on action kind
        Precondition precondition = buildPrecondition(doc, actionKind);
        if (precondition != null) {
            writeBuilder.setCurrentDocument(precondition);
        }

        return writeBuilder.build();
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
     * @param actionKind The type of action being performed
     * @return A Write operation
     */
    private Write createPutWrite(Document doc, String documentPath, ActionKind actionKind) {
        // Build the document with fields from FSCodec
        com.google.firestore.v1.Document.Builder docBuilder = com.google.firestore.v1.Document.newBuilder();
        docBuilder.setName(documentPath);
        docBuilder.putAllFields(FSCodec.encodeDoc(doc));

        // Create a write operation with the document
        Write.Builder writeBuilder = Write.newBuilder()
                .setUpdate(docBuilder.build());

        // Add precondition based on action kind
        Precondition precondition = buildPrecondition(doc, actionKind);
        if (precondition != null) {
            writeBuilder.setCurrentDocument(precondition);
        }

        return writeBuilder.build();
    }

    /**
     * Creates a Write operation for an UPDATE action with field modifications.
     *
     * @param doc The document to update
     * @param documentPath Full path to the document
     * @param mods Map of field modifications
     * @return A Write operation for update
     */
    private Write createUpdateWrite(Document doc, String documentPath, Map<String, Object> mods) {
        if (mods == null || mods.isEmpty()) {
            // If no modifications, treat as a PUT operation
            return createPutWrite(doc, documentPath, ActionKind.ACTION_KIND_PUT);
        }

        // Create the document with only the fields to update
        com.google.firestore.v1.Document.Builder docBuilder = com.google.firestore.v1.Document.newBuilder()
                .setName(documentPath);

        // Create update mask with field paths
        List<String> fieldPaths = new ArrayList<>();

        for (Map.Entry<String, Object> entry : mods.entrySet()) {
            String fieldPath = entry.getKey();
            Object value = entry.getValue();
            
            fieldPaths.add(fieldPath);
            
            if (value != null) {
                Value firestoreValue = FSCodec.encodeValue(value);
                if (firestoreValue != null) {
                    docBuilder.putFields(fieldPath, firestoreValue);
                }
            }
        }

        // Create update mask
        DocumentMask updateMask = DocumentMask.newBuilder()
                .addAllFieldPaths(fieldPaths)
                .build();

        // Create write operation
        Write.Builder writeBuilder = Write.newBuilder()
                .setUpdate(docBuilder.build())
                .setUpdateMask(updateMask);

        // Add precondition - for updates, we typically want the document to exist
        Precondition precondition = buildPrecondition(doc, ActionKind.ACTION_KIND_UPDATE);
        if (precondition != null) {
            writeBuilder.setCurrentDocument(precondition);
        }

        return writeBuilder.build();
    }

    /**
     * Builds a precondition for write operations based on action kind and revision.
     *
     * @param doc The document
     * @param actionKind The type of action being performed
     * @return A Firestore precondition or null if no precondition applies
     */
    private Precondition buildPrecondition(Document doc, ActionKind actionKind) {
        switch (actionKind) {
            case ACTION_KIND_CREATE:
                // Precondition: the document doesn't already exist
                return Precondition.newBuilder().setExists(false).build();
            case ACTION_KIND_REPLACE:
            case ACTION_KIND_UPDATE:
                // Precondition: the revision matches, or if there is no revision, the document exists
                Precondition revisionPrecondition = buildRevisionPrecondition(doc);
                if (revisionPrecondition != null) {
                    return revisionPrecondition;
                }
                // If no revision, just check that document exists
                return Precondition.newBuilder().setExists(true).build();
            case ACTION_KIND_PUT:
            case ACTION_KIND_DELETE:
                // Precondition: the revision matches, if any
                return buildRevisionPrecondition(doc);
            case ACTION_KIND_GET:
                // No preconditions on a Get
                return null;
            default:
                throw new IllegalArgumentException("Invalid action kind: " + actionKind);
        }
    }
    
    /**
     * Builds a revision-based precondition using the timestamp in the revision field.
     *
     * @param doc The document
     * @return A Firestore precondition based on update timestamp or null if no revision
     */
    private Precondition buildRevisionPrecondition(Document doc) {
        Object revisionObject = doc.getField(getRevisionField());
        if (!(revisionObject instanceof Timestamp)) {
            return null;
        }

        Timestamp revision = (Timestamp) revisionObject;
        // Create a Firestore updateTime precondition using the timestamp
        return Precondition.newBuilder()
                .setUpdateTime(revision)
                .build();

    }

    /**
     * Processes atomic writes using Firestore transactions.
     * <p>
     * Ensures that a set of operations either all succeed or all fail.
     * Unlike batched writes which may be split into multiple commits for performance,
     * atomic writes MUST be executed in a single commit to guarantee true atomicity.
     *
     * @param writes List of atomic write actions
     * @param beforeDo Optional callback executed before each write
     */
    private void runAtomicWrites(List<Action> writes, Consumer<Predicate<Object>> beforeDo) {
        if (writes.isEmpty()) {
            return;
        }

        // Build atomic write commit call following the Go pattern
        AtomicWriteCommitCall atomicWriteCall = buildAtomicWritesCommitCall(writes, beforeDo);
        
        // Execute the atomic write commit call - MUST be a single commit
        if (!atomicWriteCall.getWrites().isEmpty()) {
            doAtomicCommitCall(atomicWriteCall);
        }
    }

    /**
     * Construct a commit call with all the atomic writes.
     * This follows the Go Cloud pattern from buildAtomicWritesCommitCall.
     * All writes MUST go into a single commit for true atomicity.
     *
     * @param actions List of atomic write actions
     * @param beforeDo Optional callback executed before each write
     * @return AtomicWriteCommitCall containing all writes to be committed atomically
     */
    private AtomicWriteCommitCall buildAtomicWritesCommitCall(List<Action> actions, Consumer<Predicate<Object>> beforeDo) {
        List<Write> allWrites = new ArrayList<>();
        List<Action> processedActions = new ArrayList<>();

        for (Action action : actions) {
            // Execute beforeDo callback for this action
            if (beforeDo != null) {
                Key key = getKey(action.getDocument());
                beforeDo.accept(k -> k.equals(key));
            }

            // Convert action to write operations
            Write write = actionToWrite(action);
            if (write != null) {
                allWrites.add(write);
                processedActions.add(action);
            }
        }

        return new AtomicWriteCommitCall(allWrites, processedActions);
    }

    /**
     * Convert an action to a Firestore Write operation.
     * This follows the Go Cloud pattern from actionToWrites.
     *
     * @param action The action to convert
     * @return A Firestore Write operation
     */
    private Write actionToWrite(Action action) {
        Key key = getKey(action.getDocument());
        String documentPath = getDocumentPath(key.getDocumentId());

        switch (action.getKind()) {
            case ACTION_KIND_CREATE:
                return createPutWrite(action.getDocument(), documentPath, action.getKind());
                
            case ACTION_KIND_REPLACE:
                return createPutWrite(action.getDocument(), documentPath, action.getKind());
                
            case ACTION_KIND_PUT:
                return createPutWrite(action.getDocument(), documentPath, action.getKind());
                
            case ACTION_KIND_UPDATE:
                // For updates, we need to handle field modifications
                return createUpdateWrite(action.getDocument(), documentPath, action.getMods());
                
            case ACTION_KIND_DELETE:
                return createDeleteWrite(action.getDocument(), documentPath, action.getKind());
                
            default:
                throw new IllegalArgumentException("Unknown action kind: " + action.getKind());
        }
    }

    /**
     * Execute the atomic write commit call.
     * This follows the Go Cloud pattern from doCommitCall.
     * CRITICAL: All writes must be in a single commit for atomicity.
     *
     * @param atomicWriteCall The atomic write commit call to execute
     */
    private void doAtomicCommitCall(AtomicWriteCommitCall atomicWriteCall) {
        // CRITICAL: Create a single CommitRequest with ALL atomic writes
        // This ensures true atomicity - all succeed or all fail together
        CommitRequest commitRequest = CommitRequest.newBuilder()
                .setDatabase(getDatabasePath())
                .addAllWrites(atomicWriteCall.getWrites())
                .build();

        // Execute the commit request - if this fails, ALL writes are rolled back
        CommitResponse response;
        try {
            response = firestoreClient.commit(commitRequest);
        } catch (ApiException e) {
            // Handle precondition failures and other errors
            if (e.getCause() != null && e.getCause().getMessage().contains("FAILED_PRECONDITION")) {
                // Determine which exception to throw based on the first action's kind
                if (!atomicWriteCall.getActions().isEmpty()) {
                    ActionKind firstActionKind = atomicWriteCall.getActions().get(0).getKind();
                    if (firstActionKind == ActionKind.ACTION_KIND_CREATE) {
                        throw new ResourceAlreadyExistsException("Atomic write failed: document already exists", e);
                    } else {
                        throw new ResourceNotFoundException("Atomic write failed: document not found", e);
                    }
                }
            }
            throw new TransactionFailedException("Atomic write failed - all operations rolled back", e);
        }

        // Update revision fields in all documents using the update time from the response
        for (int i = 0; i < response.getWriteResultsCount() && i < atomicWriteCall.getActions().size(); i++) {
            Action action = atomicWriteCall.getActions().get(i);
            // TODO: When we support update, this will need to adapt the
            // update being reflected in two items in write results
            if (action.getDocument().hasField(getRevisionField())) {
                Timestamp updateTime = response.getWriteResults(i).getUpdateTime();
                action.getDocument().setField(getRevisionField(), updateTime);
            }
        }
    }

    /**
     * Helper class to hold atomic write commit call information.
     * This follows the Go Cloud pattern from commitCall struct.
     */
    private static class AtomicWriteCommitCall {
        private final List<Write> writes;
        private final List<Action> actions;

        public AtomicWriteCommitCall(List<Write> writes, List<Action> actions) {
            this.writes = writes;
            this.actions = actions;
        }

        public List<Write> getWrites() {
            return writes;
        }

        public List<Action> getActions() {
            return actions;
        }
    }

    /**
     * Plans a Firestore structured query based on the provided query parameters.
     *
     * @param query The query to plan for execution
     * @return A QueryRunner configured to execute the query
     */
    protected QueryRunner planQuery(Query query) {
        // Create a structured query builder
        StructuredQuery.Builder structuredQueryBuilder = StructuredQuery.newBuilder();
        
        // Set the collection to query
        structuredQueryBuilder.addFrom(
            StructuredQuery.CollectionSelector.newBuilder()
                .setCollectionId(extractCollectionId(collectionOptions.getTableName()))
                .build()
        );
        
        // Handle field projections (select specific fields)
        if (query.getFieldPaths() != null && !query.getFieldPaths().isEmpty()) {
            StructuredQuery.Projection.Builder projectionBuilder = StructuredQuery.Projection.newBuilder();
            
            // Add each field to the projection
            for (String fieldPath : query.getFieldPaths()) {
                projectionBuilder.addFields(
                    StructuredQuery.FieldReference.newBuilder()
                        .setFieldPath(fieldPath)
                        .build()
                );
            }
        }
        
        // Handle query filters
        if (query.getFilters() != null && !query.getFilters().isEmpty()) {
            StructuredQuery.Filter filter = filtersToStructuredFilter(query.getFilters());
            if (filter != null) {
                structuredQueryBuilder.setWhere(filter);
            }
        }
        
        // Handle ordering
        if (query.getOrderByField() != null && !query.getOrderByField().isEmpty()) {
            StructuredQuery.Direction direction = query.isOrderAscending() ? 
                StructuredQuery.Direction.ASCENDING : StructuredQuery.Direction.DESCENDING;
            
            structuredQueryBuilder.addOrderBy(
                StructuredQuery.Order.newBuilder()
                    .setField(
                        StructuredQuery.FieldReference.newBuilder()
                            .setFieldPath(query.getOrderByField())
                            .build()
                    )
                    .setDirection(direction)
                    .build()
            );
        }
        
        // Handle offset
        if (query.getOffset() > 0) {
            structuredQueryBuilder.setOffset(query.getOffset());
        }
        
        // Handle limit
        if (query.getLimit() > 0) {
            structuredQueryBuilder.setLimit(
                com.google.protobuf.Int32Value.newBuilder()
                    .setValue(query.getLimit())
                    .build()
            );
        }
        
        // Build the run query request
        RunQueryRequest request = RunQueryRequest.newBuilder()
            .setParent(getDatabasePath() + "/documents")
            .setStructuredQuery(structuredQueryBuilder.build())
            .build();
        
        return new QueryRunner(
            firestoreClient,
            request,
            query.getBeforeQuery()
        );
    }
    
    /**
     * Converts a list of filters to a Firestore structured filter.
     *
     * @param filters The list of filters to convert
     * @return A Firestore structured filter
     */
    private StructuredQuery.Filter filtersToStructuredFilter(List<Filter> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        // Handle it specially because we don't need the composite filter condition for single filter
        if (filters.size() == 1) {
            return filterToStructuredFilter(filters.get(0));
        }
        
        // Combine multiple filters with AND
        StructuredQuery.CompositeFilter.Builder compositeFilter = StructuredQuery.CompositeFilter.newBuilder()
            .setOp(StructuredQuery.CompositeFilter.Operator.AND);
        
        for (Filter filter : filters) {
            StructuredQuery.Filter structuredFilter = filterToStructuredFilter(filter);
            if (structuredFilter != null) {
                compositeFilter.addFilters(structuredFilter);
            }
        }
        
        return StructuredQuery.Filter.newBuilder()
            .setCompositeFilter(compositeFilter.build())
            .build();
    }
    
    /**
     * Converts a single filter to a Firestore structured filter.
     *
     * @param filter The filter to convert
     * @return A Firestore structured filter
     */
    private StructuredQuery.Filter filterToStructuredFilter(Filter filter) {
        if (filter == null) {
            return null;
        }
        
        String fieldPath = filter.getFieldPath();
        FilterOperation op = filter.getOp();
        Object value = filter.getValue();
        
        // Create field reference
        StructuredQuery.FieldReference fieldRef = StructuredQuery.FieldReference.newBuilder()
            .setFieldPath(fieldPath)
            .build();
        
        // Convert value to Firestore Value
        Value firestoreValue = FSCodec.encodeValue(value);
        if (firestoreValue == null) {
            return null;
        }
        
        // Map operation to Firestore operator
        StructuredQuery.FieldFilter.Operator operator;
        switch (op) {
            case EQUAL:
                operator = StructuredQuery.FieldFilter.Operator.EQUAL;
                break;
            case NOT_IN:
                operator = StructuredQuery.FieldFilter.Operator.NOT_IN;
                break;
            case GREATER_THAN:
                operator = StructuredQuery.FieldFilter.Operator.GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                operator = StructuredQuery.FieldFilter.Operator.GREATER_THAN_OR_EQUAL;
                break;
            case LESS_THAN:
                operator = StructuredQuery.FieldFilter.Operator.LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL_TO:
                operator = StructuredQuery.FieldFilter.Operator.LESS_THAN_OR_EQUAL;
                break;
            case IN:
                operator = StructuredQuery.FieldFilter.Operator.IN;
                break;
            default:
                return null;
        }
        
        return StructuredQuery.Filter.newBuilder()
            .setFieldFilter(
                StructuredQuery.FieldFilter.newBuilder()
                    .setField(fieldRef)
                    .setOp(operator)
                    .setValue(firestoreValue)
                    .build()
            )
            .build();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Executes a query against Firestore and returns an iterator for the results.
     *
     * @param query The query to execute
     * @return An iterator for the query results
     */
    @Override
    public DocumentIterator runGetQuery(Query query) {
        QueryRunner queryRunner = planQuery(query);
        if (queryRunner == null) {
            throw new SubstrateSdkException("Failed to plan query for execution");
        }
        
        // Create the document iterator - offset/limit are handled in the query now
        return new FSDocumentIterator(queryRunner, 0, 0);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Generates a query execution plan for the given query.
     *
     * @param query The query to plan
     * @return A string representation of the query plan
     */
    @Override
    public String queryPlan(Query query) {
        QueryRunner queryRunner = planQuery(query);
        if (queryRunner == null) {
            return "Failed to plan query for execution";
        }
        return queryRunner.queryPlan();
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
        executorService.shutdown();
        firestoreClient.close();
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
     * @param start The starting index (inclusive) of the batch
     * @param end The ending index (inclusive) of the batch
     */
    @Override
    protected void batchGet(List<Action> gets, Consumer<Predicate<Object>> beforeDo, int start, int end) {
        if (gets.isEmpty() || start > end || start < 0 || end >= gets.size()) {
            return;
        }

        // Build a list of document references for batch retrieval
        List<String> documentPaths = new ArrayList<>();
        Map<String, Action> docPathToAction = new HashMap<>();

        for (int i = start; i <= end; i++) {
            Action action = gets.get(i);
            Key key = (Key) action.getKey();
            String documentPath = getDocumentPath(key.getDocumentId());
            documentPaths.add(documentPath);
            docPathToAction.put(documentPath, action);
        }

        if (beforeDo != null) {
            beforeDo.accept(object -> true);
        }

        try {
            // Create a batch get request using Firestore V1 API
            BatchGetDocumentsRequest.Builder requestBuilder =
                BatchGetDocumentsRequest.newBuilder()
                    .setDatabase(getDatabasePath())
                    .addAllDocuments(documentPaths);

            // Add field mask if specific fields are requested
            if (gets.get(start).getFieldPaths() != null && !gets.get(start).getFieldPaths().isEmpty()) {
                DocumentMask.Builder maskBuilder = DocumentMask.newBuilder();
                // Add key fields to ensure they're included
                Set<String> fieldPaths = new HashSet<>(gets.get(start).getFieldPaths());
                fieldPaths.add(collectionOptions.getPartitionKey());
                if (collectionOptions.getSortKey() != null) {
                    fieldPaths.add(collectionOptions.getSortKey());
                }
                
                maskBuilder.addAllFieldPaths(fieldPaths);
                requestBuilder.setMask(maskBuilder.build());
            }

            // Execute the batch get request
            BatchGetDocumentsRequest request = requestBuilder.build();

            // Process each document response
            firestoreClient.batchGetDocumentsCallable().call(request).forEach(response -> {
                if (response.hasFound()) {
                    com.google.firestore.v1.Document foundDoc = response.getFound();
                    String documentPath = foundDoc.getName();
                    Action action = docPathToAction.get(documentPath);
                    
                    if (action != null) {
                        // Decode document fields into the action's document
                        FSCodec.decodeDoc(foundDoc, action.getDocument(), getRevisionField());
                    }
                }
                // Missing documents are silently ignored, which matches other implementations
            });
        } catch (Exception e) {
            throw new SubstrateSdkException("Error executing Firestore batch gets", e);
        }
    }

    /**
     * Extracts the collection ID (last segment) from a full collection path.
     *
     * @param tableName The full collection path or name
     * @return The collection ID (last segment of the path)
     */
    private String extractCollectionId(String tableName) {
        // Handle paths that contain slash characters
        int lastSlashIndex = tableName.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < tableName.length() - 1) {
            return tableName.substring(lastSlashIndex + 1);
        }
        return tableName;
    }
}

