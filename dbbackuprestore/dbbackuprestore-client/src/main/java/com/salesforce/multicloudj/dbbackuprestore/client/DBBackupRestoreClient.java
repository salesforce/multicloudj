package com.salesforce.multicloudj.dbbackuprestore.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;

import java.util.List;
import java.util.ServiceLoader;

/**
 * Client for performing database backup and restore operations across multiple cloud providers.
 * This class provides a cloud-agnostic interface for backup and restore operations.
 *
 * @since 0.2.25
 */
public class DBBackupRestoreClient implements AutoCloseable {

    private final AbstractDBBackupRestore dbBackupRestore;

    /**
     * Creates a new DBBackupRestoreClient wrapping the given driver.
     *
     * @param dbBackupRestore the database backup restore driver implementation
     */
    public DBBackupRestoreClient(AbstractDBBackupRestore dbBackupRestore) {
        this.dbBackupRestore = dbBackupRestore;
    }

    /**
     * Creates a new builder for constructing a DBBackupRestoreClient.
     *
     * @param providerId the cloud provider identifier
     * @return a new ClientBuilder instance
     */
    public static DBBackupRestoreClientBuilder builder(String providerId) {
        return new DBBackupRestoreClientBuilder(providerId);
    }

    /**
     * Returns an Iterable of all available AbstractDBBackupRestore implementations.
     *
     * @return an Iterable of AbstractDBBackupRestore instances
     */
    private static Iterable<AbstractDBBackupRestore> all() {
        ServiceLoader<AbstractDBBackupRestore> services = ServiceLoader.load(AbstractDBBackupRestore.class);
        ImmutableSet.Builder<AbstractDBBackupRestore> builder = ImmutableSet.builder();
        for (AbstractDBBackupRestore service : services) {
            builder.add(service);
        }
        return builder.build();
    }

    /**
     * Finds the builder for the specified provider.
     *
     * @param providerId the ID of the provider
     * @return the AbstractDBBackupRestore.Builder for the specified provider
     * @throws IllegalArgumentException if no provider is found for the given ID
     */
    private static AbstractDBBackupRestore.Builder<?, ?> findProviderBuilder(String providerId) {
        for (AbstractDBBackupRestore provider : all()) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException(
                "No DBBackupRestore provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     *
     * @param provider the AbstractDBBackupRestore provider
     * @return the AbstractDBBackupRestore.Builder for the provider
     * @throws RuntimeException if the builder creation fails
     */
    private static AbstractDBBackupRestore.Builder<?, ?> createBuilderInstance(
            AbstractDBBackupRestore provider) {
        try {
            Class<? extends AbstractDBBackupRestore> providerClass = provider.getClass();
            Class<?>[] innerClasses = providerClass.getDeclaredClasses();
            for (Class<?> innerClass : innerClasses) {
                if (AbstractDBBackupRestore.Builder.class.isAssignableFrom(innerClass)) {
                    return (AbstractDBBackupRestore.Builder<?, ?>) innerClass.getDeclaredConstructor()
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
        return dbBackupRestore.listBackups();
    }

    /**
     * Gets details of a specific backup by its ID.
     *
     * @param backupId the unique identifier of the backup
     * @return the Backup object with full metadata
     */
    public Backup getBackup(String backupId) {
        return dbBackupRestore.getBackup(backupId);
    }

    /**
     * Gets the current status of a specific backup.
     * This is a convenience method that retrieves only the status without full backup metadata.
     *
     * @param backupId the unique identifier of the backup
     * @return the current BackupStatus of the backup
     */
    public BackupStatus getBackupStatus(String backupId) {
        return dbBackupRestore.getBackupStatus(backupId);
    }

    /**
     * Restores a table from a backup.
     * The restore operation may take time depending on the backup size and is async request.
     *
     * @param request the restore request containing restore configuration
     */
    public void restoreBackup(RestoreRequest request) {
        dbBackupRestore.restoreBackup(request);
    }

    @Override
    public void close() throws Exception {
        if (dbBackupRestore != null) {
            dbBackupRestore.close();
        }
    }

    /**
     * Builder class for constructing DBBackupRestoreClient instances.
     */
    public static class DBBackupRestoreClientBuilder {
        private final AbstractDBBackupRestore.Builder<?, ?> dbBackupRestoreBuilder;

        /**
         * Creates a new ClientBuilder.
         *
         * @param providerId the cloud provider identifier
         */
        public DBBackupRestoreClientBuilder(String providerId) {
            this.dbBackupRestoreBuilder = findProviderBuilder(providerId);
        }

        /**
         * Sets the region for the client.
         *
         * @param region the region where the database is located
         * @return this builder
         */
        public DBBackupRestoreClientBuilder withRegion(String region) {
            this.dbBackupRestoreBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets the table name for backup/restore operations.
         *
         * @param resourceName the name of the table
         * @return this builder
         */
        public DBBackupRestoreClientBuilder withResourceName(String resourceName) {
            this.dbBackupRestoreBuilder.withResourceName(resourceName);
            return this;
        }

        /**
         * Builds and returns a new DBBackupRestoreClient instance.
         *
         * @return a new DBBackupRestoreClient
         */
        public DBBackupRestoreClient build() {
            AbstractDBBackupRestore dbBackupRestore = this.dbBackupRestoreBuilder.build();
            return new DBBackupRestoreClient(dbBackupRestore);
        }
    }
}
