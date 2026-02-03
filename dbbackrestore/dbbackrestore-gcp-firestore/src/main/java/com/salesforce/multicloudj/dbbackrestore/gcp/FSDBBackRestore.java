package com.salesforce.multicloudj.dbbackrestore.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.firestore.admin.v1.ListBackupsResponse;
import com.google.auto.service.AutoService;
import com.google.firestore.admin.v1.RestoreDatabaseRequest;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * GCP Firestore implementation of database backup and restore operations.
 * This implementation uses the Firestore Admin API for backup/restore operations.
 *
 * @since 0.2.25
 */
@AutoService(AbstractDBBackRestore.class)
public class FSDBBackRestore extends AbstractDBBackRestore {

    private FirestoreAdminClient firestoreAdminClient;

    /**
     * Default constructor for ServiceLoader.
     */
    public FSDBBackRestore() {
        super(new Builder());
    }

    /**
     * Constructs a FSDBBackRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public FSDBBackRestore(Builder builder) {
        super(builder);
        this.firestoreAdminClient = builder.firestoreAdminClient;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // GCP exceptions are typically ApiException
        if (t instanceof ApiException) {
            ApiException apiException = (ApiException) t;
            // Map common GCP error codes
            switch (apiException.getStatusCode().getCode()) {
                case NOT_FOUND:
                    return ResourceNotFoundException.class;
                case ALREADY_EXISTS:
                    return ResourceAlreadyExistsException.class;
                case PERMISSION_DENIED:
                case UNAUTHENTICATED:
                    return UnAuthorizedException.class;
                default:
                    return UnknownException.class;
            }
        }
        return UnknownException.class;
    }

    @Override
    public List<Backup> listBackups() {
        ListBackupsResponse response = firestoreAdminClient.listBackups(region);
        List<Backup> backups = new ArrayList<>();
        for (com.google.firestore.admin.v1.Backup backup : response.getBackupsList()) {
            backups.add(convertToBackup(backup));
        }
        return backups;
    }

    @Override
    public Backup getBackup(String backupId) {
        com.google.firestore.admin.v1.Backup backup = firestoreAdminClient.getBackup(backupId);
        return convertToBackup(backup);
    }

    @Override
    public BackupStatus getBackupStatus(String backupId) {
        Backup backup = getBackup(backupId);
        return backup.getStatus();
    }

    @Override
    public void restoreBackup(RestoreRequest request) {
        // Build restore request
        String targetDBID = request.getTargetResource();
        if (StringUtils.isBlank(targetDBID)) {
            throw new IllegalArgumentException("target database ID cannot be empty");
        }

        // Extract parent from backup ID (format: projects/{project}/locations/{location}/backups/{backup})
        String parent = request.getBackupId().substring(
                0, request.getBackupId().lastIndexOf("/backups/"));
        parent = parent.replace("/locations/", "/");

        RestoreDatabaseRequest.Builder restoreBuilder = RestoreDatabaseRequest.newBuilder()
                .setParent(parent)
                .setDatabaseId(targetDBID)
                .setBackup(request.getBackupId());

        // Restore is a long-running operation
        firestoreAdminClient.restoreDatabaseAsync(restoreBuilder.build());
    }

    @Override
    public void close() {
        if (firestoreAdminClient != null) {
            firestoreAdminClient.close();
        }
    }

    /**
     * Converts Firestore Admin API Backup to MultiCloudJ Backup model.
     */
    private Backup convertToBackup(com.google.firestore.admin.v1.Backup backup) {
        return Backup.builder()
                .backupId(backup.getName())
                .resourceName(backup.getDatabase())
                .status(convertBackupState(backup.getState()))
                .creationTime(Instant.ofEpochSecond(
                        backup.getSnapshotTime().getSeconds(),
                        backup.getSnapshotTime().getNanos()))
                .expiryTime(Instant.ofEpochSecond(
                        backup.getExpireTime().getSeconds(),
                        backup.getExpireTime().getNanos()))
                .sizeInBytes(backup.getStats().getSizeBytes())
                .build();
    }

    /**
     * Converts Firestore Backup.State to MultiCloudJ BackupStatus.
     */
    private BackupStatus convertBackupState(com.google.firestore.admin.v1.Backup.State state) {
        switch (state) {
            case CREATING:
                return BackupStatus.CREATING;
            case READY:
                return BackupStatus.AVAILABLE;
            case NOT_AVAILABLE:
                return BackupStatus.FAILED;
            default:
                return BackupStatus.UNKNOWN;
        }
    }

    /**
     * Builder for FSDBBackRestore.
     */
    public static class Builder extends AbstractDBBackRestore.Builder<FSDBBackRestore, Builder> {
        private FirestoreAdminClient firestoreAdminClient;
        private GoogleCredentials credentials;

        /**
         * Default constructor.
         */
        public Builder() {
            this.providerId = "gcp-firestore";
        }

        /**
         * Sets the Firestore Admin client for backup/restore operations.
         *
         * @param firestoreAdminClient the Firestore Admin client to use
         * @return this builder
         */
        public Builder withFirestoreAdminClient(FirestoreAdminClient firestoreAdminClient) {
            this.firestoreAdminClient = firestoreAdminClient;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public FSDBBackRestore build() {
            if (StringUtils.isBlank(region)) {
                throw new IllegalArgumentException("Region is required");
            }
            if (StringUtils.isBlank(resourceName)) {
                throw new IllegalArgumentException("Collection name is required");
            }

            // Create FirestoreAdminClient if not provided
            if (firestoreAdminClient == null) {
                try {
                    FirestoreAdminSettings.Builder settingsBuilder = FirestoreAdminSettings.newBuilder();

                    if (credentials != null) {
                        settingsBuilder.setCredentialsProvider(() -> credentials);
                    }

                    firestoreAdminClient = FirestoreAdminClient.create(settingsBuilder.build());
                } catch (IOException e) {
                    throw new UnknownException("Failed to create FirestoreAdminClient", e);
                }
            }

            return new FSDBBackRestore(this);
        }
    }
}
