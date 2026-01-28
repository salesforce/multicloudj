package com.salesforce.multicloudj.docstore.driver;

import com.google.common.base.Strings;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Base class for substrate-specific implementations.
 * This class serves the purpose of providing common (i.e. substrate-agnostic) functionality.
 * such as validation
 */
public abstract class AbstractDocStore implements Provider, Collection, AutoCloseable {

    private final String providerId;
    protected final String region;
    protected final String instanceId;
    protected final String endpointType;
    protected final CredentialsOverrider credentialsOverrider;
    protected final URI endpoint;
    protected final CollectionOptions collectionOptions;

    private final Object dsLock = new Object();
    protected boolean closed = false;

    // DefaultRevisionField is the default name of the document field used for document revision
    // information.
    private static String defaultRevisionField = "DocstoreRevision";

    protected ExecutorService executorService;

    protected AbstractDocStore(Builder<?, ?> builder) {
        this(builder.providerId, builder.collection, builder.region,
                builder.instanceId, builder.endpointType, builder.endpoint, builder.collectionOptions, builder.credentialsOverrider);
        if (collectionOptions == null || collectionOptions.getMaxOutstandingActionRPCs() < 1) {
            executorService = Executors.newCachedThreadPool();
        } else {
            executorService = Executors.newFixedThreadPool(collectionOptions.getMaxOutstandingActionRPCs());
        }
    }

    protected AbstractDocStore(String providerId, Collection collection, String region, String instanceId,
                            String endpointType, URI endpoint, CollectionOptions collectionOptions, CredentialsOverrider credentials) {
        this.providerId = providerId;
        this.region = region;
        this.credentialsOverrider = credentials;
        this.instanceId = instanceId;
        this.endpointType = Strings.nullToEmpty(endpointType);
        this.collectionOptions = collectionOptions;
        this.endpoint = endpoint;
    }

    // RevisionField returns the name of the field used to hold revisions.
    // If the empty string is returned, docstore.DefaultRevisionField will be used.
    @Override
    public String getRevisionField() {
        if (collectionOptions.getRevisionField() != null) {
            return collectionOptions.getRevisionField();
        } else {
            return defaultRevisionField;
        }
    }

    @Override
    public String getProviderId() {
        return providerId;
    }

    public ActionList getActions() {
        return new ActionList(this);
    }

    public void checkClosed() {
        synchronized (dsLock) {
            if (closed) {
                throw new IllegalStateException("DocStore has been closed");
            }
        }
    }

    // Preprocess on one action including:
    // Calculate and store the key to avoid duplicate calculation
    // Validate the action.
    protected void toDriverAction(Action action) {
        Document doc = action.getDocument();
        Object key = getKey(doc);
        if (key == null && action.getKind() != ActionKind.ACTION_KIND_CREATE) {
            throw new IllegalArgumentException("Document key should not be null: " + action.getDocument().toString());
        }

        action.setKey(key);
        Object revision = doc.getField(getRevisionField());
        if (action.getKind() == ActionKind.ACTION_KIND_CREATE && revision != null) {
            throw new IllegalArgumentException("cannot create a document with a revision field: " + action.getDocument().toString());
        }

        if (action.getKind() == ActionKind.ACTION_KIND_PUT && revision != null) {
            // A Put with a revision field is equivalent to a Replace.
            action.setKind(ActionKind.ACTION_KIND_REPLACE);
        }

        if (action.getFieldPaths() != null) {
            for (String path : action.getFieldPaths()) {
                Util.validateFieldPath(path);
            }
        }

        if (action.getKind() == ActionKind.ACTION_KIND_UPDATE) {
            toDriverMods(action.getMods());
        }
    }

    private void toDriverMods(Map<String, Object> mods) {
        if (mods == null || mods.isEmpty()) {
            throw new IllegalArgumentException("mods should not be null or empty");
        }

        for (String fieldPath : mods.keySet()) {
            Util.validateFieldPath(fieldPath);
        }

        // Docstore sorted the mods by field path, but we don't do it here.
    }

    public void runGets(List<Action> actions, Consumer<Predicate<Object>> beforeDo, int batchSize) throws ExecutionException, InterruptedException {
        if (actions.isEmpty()) {
            return;
        }
        List<List<Action>> groups = Util.groupByFieldPath(actions);

        List<Future<?>> futures = new ArrayList<>(); // To capture each task's result

        for (List<Action> group : groups) {
            int n = (group.size() + batchSize - 1) / batchSize; // compute the total number of batches

            for (int index = 0; index < n; index++) {
                int start = index * batchSize;
                int end = Math.min((index + 1) * batchSize, group.size()) - 1; // Avoid exceeding size

                // Submit each batchGet call as a task
                Future<?> future = executorService.submit(() -> batchGet(group, beforeDo, start, end));
                futures.add(future);
            }
        }

        waitTermination(futures);
    }

    protected abstract void batchGet(List<Action> actions, Consumer<Predicate<Object>> beforeDo, int start, int end);

    private void waitTermination(List<Future<?>> futures) throws ExecutionException, InterruptedException {
        // Wait for all tasks to complete and check for exceptions
        for (Future<?> future : futures) {
                future.get(); // This will rethrow any exception that occurred in the task
        }
    }

    /**
     * Lists all available backups for the collection/table.
     *
     * @return a list of Backup objects representing available backups
     * @since 0.2.26
     */
    public abstract List<Backup> listBackups();

    /**
     * Gets details of a specific backup by its ID.
     *
     * @param backupId the unique identifier of the backup
     * @return the Backup object with full metadata
     * @since 0.2.26
     */
    public abstract Backup getBackup(String backupId);

    /**
     * Gets the current status of a specific backup.
     *
     * @param backupId the unique identifier of the backup
     * @return the current BackupStatus of the backup
     * @since 0.2.26
     */
    public abstract BackupStatus getBackupStatus(String backupId);

    /**
     * Restores a collection/table from a backup.
     *
     * @param request the restore request containing restore configuration
     * @since 0.2.26
     */
    public abstract void restoreBackup(RestoreRequest request);

    /**
     * Deletes a specific backup.
     *
     * @param backupId the unique identifier of the backup to delete
     * @since 0.2.26
     */
    public abstract void deleteBackup(String backupId);

    public abstract static class Builder<A extends  AbstractDocStore, T extends Builder<A, T>> implements Provider.Builder {

        private String providerId;
        /**
         * -- GETTER --
         * Gets the collection
         *
         */
        @Getter
        private Collection collection;
        /**
         * -- GETTER --
         * Gets the region.
         *
         */
        @Getter
        private String region;

        @Getter
        private String instanceId;
        /**
         * -- GETTER --
         * Gets the endpoint.
         *
         */
        @Getter
        private URI endpoint;

        /**
         * -- GETTER --
         * Gets the endpoint type.
         *
         */
        @Getter
        private String endpointType;
        /**
         * -- GETTER --
         * Gets the CredentialsOverrider.
         *
         */
        @Getter
        private CredentialsOverrider credentialsOverrider = null;

        /**
         * -- GETTER --
         * Gets the CredentialsOverrider.
         *
         * @return The CredentialsOverrider.
         */
        @Getter
        private CollectionOptions collectionOptions = null;


        @Override
        public T providerId(String providerId) {
            this.providerId = providerId;
            return self();
        }

        /**
         * Method to supply collection
         *
         * @param collection
         * @return An instance of self
         */
        public T withCollection(Collection collection) {
            this.collection = collection;
            return self();
        }

        /**
         * Method to supply region
         *
         * @param region Region
         * @return An instance of self
         */
        public T withRegion(String region) {
            this.region = region;
            return self();
        }

        public T withInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return self();
        }

        /**
         * Method to supply an endpoint override
         *
         * @param endpoint The endpoint to set.
         * @return An instance of self
         */
        public T withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return self();
        }

        /**
         * Method to supply an endpoint override
         *
         * @param endpointType The endpoint type to set.
         * @return An instance of self
         */
        public T withEndpointType(String endpointType) {
            this.endpointType = endpointType;
            return self();
        }

        /**
         * Method to supply an credentialsOverrider override
         *
         * @param credentialsOverrider The credentialsOverrider type to set.
         * @return An instance of self
         */
        public T withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return self();
        }

        /**
         * Method to supply collectionOptions
         *
         * @param collectionOptions collectionOptions
         * Method to supply CollectionOptions
         * @return An instance of self
         */
        public T withCollectionOptions(CollectionOptions collectionOptions) {
            this.collectionOptions = collectionOptions;
            return self();
        }

        public abstract T self();

        /**
         * Builds and returns an instance of AbstractDocStore.
         *
         * @return An instance of AbstractDocStore.
         */
        public abstract A build();
    }
}
