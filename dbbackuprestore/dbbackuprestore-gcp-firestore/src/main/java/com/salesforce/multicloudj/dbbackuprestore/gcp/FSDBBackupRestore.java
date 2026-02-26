package com.salesforce.multicloudj.dbbackuprestore.gcp;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.service.AutoService;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.ListBackupsRequest;
import com.google.firestore.admin.v1.ListBackupsResponse;
import com.google.firestore.admin.v1.RestoreDatabaseMetadata;
import com.google.firestore.admin.v1.RestoreDatabaseRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreStatus;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * GCP Firestore implementation of database backup and restore operations.
 * This implementation uses the Firestore Admin API for backup/restore operations.
 *
 * @since 0.2.25
 */
@AutoService(AbstractDBBackupRestore.class)
public class FSDBBackupRestore extends AbstractDBBackupRestore {

    private FirestoreAdminClient firestoreAdminClient;

    /**
     * Default constructor for ServiceLoader.
     */
    public FSDBBackupRestore() {
        super(new Builder());
    }

    /**
     * Constructs a FSDBBackupRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public FSDBBackupRestore(Builder builder) {
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
    public String restoreBackup(RestoreRequest request) {
        // Build restore request
        String targetDBID = request.getTargetResource();
        if (StringUtils.isBlank(targetDBID)) {
            throw new IllegalArgumentException("target database ID cannot be empty");
        }

        // Extract parent from backup ID (format: projects/{project}/locations/{location}/backups/{backup})
        String parent = request.getBackupId().substring(
                0, request.getBackupId().lastIndexOf("/locations/"));

        RestoreDatabaseRequest.Builder restoreBuilder = RestoreDatabaseRequest.newBuilder()
                .setParent(parent)
                .setDatabaseId(targetDBID)
                .setBackup(request.getBackupId());

        if (StringUtils.isNotBlank(request.getKmsEncryptionKeyId())) {
            restoreBuilder.setEncryptionConfig(
                    Database.EncryptionConfig.newBuilder()
                            .setCustomerManagedEncryption(
                                    Database.EncryptionConfig.CustomerManagedEncryptionOptions.newBuilder()
                                            .setKmsKeyName(request.getKmsEncryptionKeyId())
                                            .build())
                            .build());
        }

        // Restore is a long-running operation
        OperationFuture<Database, RestoreDatabaseMetadata> operation =
                firestoreAdminClient.restoreDatabaseAsync(restoreBuilder.build());

        // Return the operation name as the restore ID
        try {
            return operation.getName();
        } catch (ExecutionException e) {
            if (e.getCause() == null) {
                throw new SubstrateSdkException(e);
            }
            throw (RuntimeException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for restore to trigger.", e);
        }
    }

    @Override
    public Restore getRestoreJob(String restoreId) {
        Operation operation = firestoreAdminClient.getOperationsClient().getOperation(restoreId);
        return convertToRestore(operation);
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
     * Converts GCP Operation to MultiCloudJ Restore model.
     */
    private Restore convertToRestore(Operation operation) {
        RestoreStatus status;
        Instant endTime = null;
        Instant startTime = null;

        String statusMessage = null;
        if (operation.getDone()) {
            if (operation.hasError()) {
                status = RestoreStatus.FAILED;
                statusMessage = operation.getError().getMessage();
            } else {
                status = RestoreStatus.COMPLETED;
            }
        } else {
            status = RestoreStatus.RESTORING;
        }

        // Extract metadata
        String backupId = "";
        String targetResource = "";
        if (operation.hasMetadata()) {
            try {
                RestoreDatabaseMetadata metadata = operation.getMetadata()
                        .unpack(RestoreDatabaseMetadata.class);
                backupId = metadata.getBackup();
                if (metadata.hasEndTime()) {
                    endTime = Instant.ofEpochSecond(metadata.getEndTime().getSeconds(), metadata.getEndTime().getNanos());
                }
                if (metadata.hasStartTime()) {
                    startTime = Instant.ofEpochSecond(metadata.getStartTime().getSeconds(), metadata.getStartTime().getNanos());
                }
                targetResource = metadata.getDatabase();

            } catch (InvalidProtocolBufferException e) {
                throw new UnknownException("backup metadata is corrupted");
            }
        }
        return Restore.builder()
                .restoreId(operation.getName())
                .backupId(backupId)
                .targetResource(targetResource)
                .status(status)
                .startTime(startTime)
                .endTime(endTime)
                .statusMessage(statusMessage)
                .build();
    }

    /**
     * Builder for FSDBBackupRestore.
     */
    public static class Builder extends AbstractDBBackupRestore.Builder<FSDBBackupRestore, Builder> {
        private FirestoreAdminClient firestoreAdminClient;

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
        public FSDBBackupRestore build() {
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
                    firestoreAdminClient = FirestoreAdminClient.create(settingsBuilder.build());
                } catch (IOException e) {
                    throw new UnknownException("Failed to create FirestoreAdminClient", e);
                }
            }

            return new FSDBBackupRestore(this);
        }
    }
}
