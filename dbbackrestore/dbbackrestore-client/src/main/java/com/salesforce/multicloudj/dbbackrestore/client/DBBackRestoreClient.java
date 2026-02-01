package com.salesforce.multicloudj.dbbackrestore.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;

import java.util.List;
import java.util.ServiceLoader;

/**
 * Client for performing database backup and restore operations across multiple cloud providers.
 * This class provides a cloud-agnostic interface for backup and restore operations.
 *
 * @since 0.2.25
 */
public class DBBackRestoreClient implements AutoCloseable {

    private final AbstractDBBackRestore dbBackRestore;

    /**
     * Creates a new DBBackRestoreClient wrapping the given driver.
     *
     * @param dbBackRestore the database backup restore driver implementation
     */
    public DBBackRestoreClient(AbstractDBBackRestore dbBackRestore) {
        this.dbBackRestore = dbBackRestore;
    }

    /**
     * Creates a new builder for constructing a DBBackRestoreClient.
     *
     * @param providerId the cloud provider identifier
     * @return a new ClientBuilder instance
     */
    public static DBBackRestoreClientBuilder builder(String providerId) {
        return new DBBackRestoreClientBuilder(providerId);
    }

    /**
     * Returns an Iterable of all available AbstractDBBackRestore implementations.
     *
     * @return an Iterable of AbstractDBBackRestore instances
     */
    private static Iterable<AbstractDBBackRestore> all() {
        ServiceLoader<AbstractDBBackRestore> services = ServiceLoader.load(AbstractDBBackRestore.class);
        ImmutableSet.Builder<AbstractDBBackRestore> builder = ImmutableSet.builder();
        for (AbstractDBBackRestore service : services) {
            builder.add(service);
        }
        return builder.build();
    }

    /**
     * Finds the builder for the specified provider.
     *
     * @param providerId the ID of the provider
     * @return the AbstractDBBackRestore.Builder for the specified provider
     * @throws IllegalArgumentException if no provider is found for the given ID
     */
    private static AbstractDBBackRestore.Builder<?, ?> findProviderBuilder(String providerId) {
        for (AbstractDBBackRestore provider : all()) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException(
                "No DBBackRestore provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     *
     * @param provider the AbstractDBBackRestore provider
     * @return the AbstractDBBackRestore.Builder for the provider
     * @throws RuntimeException if the builder creation fails
     */
    private static AbstractDBBackRestore.Builder<?, ?> createBuilderInstance(
            AbstractDBBackRestore provider) {
        try {
            Class<? extends AbstractDBBackRestore> providerClass = provider.getClass();
            Class<?>[] innerClasses = providerClass.getDeclaredClasses();
            for (Class<?> innerClass : innerClasses) {
                if (AbstractDBBackRestore.Builder.class.isAssignableFrom(innerClass)) {
                    return (AbstractDBBackRestore.Builder<?, ?>) innerClass.getDeclaredConstructor()
                            .newInstance();
                }
            }
            throw new RuntimeException(
                    "No Builder class found in provider: " + providerClass.getName());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder instance", e);
        }
    }

    /**
     * Lists all available backups for the current table.
     *
     * @return a list of Backup objects representing available backups
     */
    public List<Backup> listBackups() {
        return dbBackRestore.listBackups();
    }

    /**
     * Gets details of a specific backup by its ID.
     *
     * @param backupId the unique identifier of the backup
     * @return the Backup object with full metadata
     */
    public Backup getBackup(String backupId) {
        return dbBackRestore.getBackup(backupId);
    }

    /**
     * Gets the current status of a specific backup.
     * This is a convenience method that retrieves only the status without full backup metadata.
     *
     * @param backupId the unique identifier of the backup
     * @return the current BackupStatus of the backup
     */
    public BackupStatus getBackupStatus(String backupId) {
        return dbBackRestore.getBackupStatus(backupId);
    }

    /**
     * Restores a table from a backup.
     * The restore operation may take time depending on the backup size and is async request.
     *
     * @param request the restore request containing restore configuration
     */
    public void restoreBackup(RestoreRequest request) {
        dbBackRestore.restoreBackup(request);
    }

    @Override
    public void close() throws Exception {
        if (dbBackRestore != null) {
            dbBackRestore.close();
        }
    }

    /**
     * Builder class for constructing DBBackRestoreClient instances.
     */
    public static class DBBackRestoreClientBuilder {
        private final AbstractDBBackRestore.Builder<?, ?> dbBackRestoreBuilder;

        /**
         * Creates a new ClientBuilder.
         *
         * @param providerId the cloud provider identifier
         */
        public DBBackRestoreClientBuilder(String providerId) {
            this.dbBackRestoreBuilder = findProviderBuilder(providerId);
        }

        /**
         * Sets the region for the client.
         *
         * @param region the region where the database is located
         * @return this builder
         */
        public DBBackRestoreClientBuilder withRegion(String region) {
            this.dbBackRestoreBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets the table name for backup/restore operations.
         *
         * @param resourceName the name of the table
         * @return this builder
         */
        public DBBackRestoreClientBuilder withResourceName(String resourceName) {
            this.dbBackRestoreBuilder.withResourceName(resourceName);
            return this;
        }

        /**
         * Builds and returns a new DBBackRestoreClient instance.
         *
         * @return a new DBBackRestoreClient
         */
        public DBBackRestoreClient build() {
            AbstractDBBackRestore dbBackRestore = this.dbBackRestoreBuilder.build();
            return new DBBackRestoreClient(dbBackRestore);
        }
    }
}
