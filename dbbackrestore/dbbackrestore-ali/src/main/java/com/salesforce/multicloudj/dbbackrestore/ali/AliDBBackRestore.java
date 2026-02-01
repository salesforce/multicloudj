package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.hbr20170908.models.*;
import com.aliyun.teaopenapi.models.Config;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Alibaba Cloud implementation of database backup and restore operations.
 * This implementation uses the HBR (Hybrid Backup Recovery) service for TableStore backups.
 *
 * <p>The HBR service manages TableStore backups through OTS (Open Table Service) snapshots.
 * Backups are created by HBR backup plans and can be restored using the CreateRestoreJob API.
 *
 * @since 0.2.25
 */
@AutoService(AbstractDBBackRestore.class)
public class AliDBBackRestore extends AbstractDBBackRestore {

    private Client hbrClient;

    /**
     * Default constructor for ServiceLoader.
     */
    public AliDBBackRestore() {
        super(new Builder());
    }

    /**
     * Constructs an AliDBBackRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public AliDBBackRestore(Builder builder) {
        super(builder);
        this.hbrClient = builder.hbrClient;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return ErrorCodeMapping.getException(t);
    }

    @Override
    public List<Backup> listBackups() {
        try {
            DescribeOtsTableSnapshotsRequest request = new DescribeOtsTableSnapshotsRequest();
            DescribeOtsTableSnapshotsResponse response = hbrClient.describeOtsTableSnapshots(request);

            if (response == null || response.getBody() == null
                    || response.getBody().getSnapshots() == null) {
                return new ArrayList<>();
            }

            List<Backup> backups = new ArrayList<>();
            for (DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot :
                    response.getBody().getSnapshots()) {
                backups.add(convertToBackup(snapshot));
            }

            return backups;
        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to list Alibaba TableStore backups", e);
        }
    }

    @Override
    public Backup getBackup(String backupId) {
        if (hbrClient == null) {
            throw new SubstrateSdkException("HBR client not initialized. "
                    + "Ensure credentials are configured in the builder.");
        }

        try {
            // List all snapshots and find the matching one
            List<Backup> backups = listBackups();

            for (Backup backup : backups) {
                if (backup.getBackupId().equals(backupId)) {
                    return backup;
                }
            }

            throw new ResourceNotFoundException("Backup not found: " + backupId);
        } catch (SubstrateSdkException e) {
            throw e;
        } catch (Exception e) {
            throw new SubstrateSdkException("Failed to get Alibaba TableStore backup: " + backupId, e);
        }
    }

    @Override
    public BackupStatus getBackupStatus(String backupId) {
        Backup backup = getBackup(backupId);
        return backup.getStatus();
    }

    @Override
    public void restoreBackup(RestoreRequest request) {
        if (hbrClient == null) {
            throw new SubstrateSdkException("HBR client not initialized. "
                    + "Ensure credentials are configured in the builder.");
        }

        try {
            CreateRestoreJobRequest restoreJobRequest = new CreateRestoreJobRequest();
            restoreJobRequest.setRestoreType("OTS_TABLE");
            restoreJobRequest.setSnapshotId(request.getBackupId());

            // Set the target table name
            String targetTableName = request.getTargetResource() != null
                    && !request.getTargetResource().isEmpty()
                    ? request.getTargetResource()
                    : getResourceName() + "-restored";

            // Configure OTS restore details
            OtsTableRestoreDetail otsDetail = new OtsTableRestoreDetail();
            otsDetail.setOverwriteExisting(false); // Don't overwrite existing tables by default
            restoreJobRequest.setOtsDetail(otsDetail);

            // Set vault ID if provided (required for restore)
            if (request.getVaultId() != null && !request.getVaultId().isEmpty()) {
                restoreJobRequest.setVaultId(request.getVaultId());
            }

            CreateRestoreJobResponse response = hbrClient.createRestoreJob(restoreJobRequest);

            if (response == null || response.getBody() == null || !response.getBody().getSuccess()) {
                String errorMsg = response != null && response.getBody() != null
                        ? response.getBody().getMessage() : "Unknown error";
                String errorCode = response != null && response.getBody() != null
                        ? response.getBody().getCode() : "UNKNOWN";
                throw new SubstrateSdkException("Failed to create restore job: "
                        + errorMsg + " (Code: " + errorCode + ")");
            }
        } catch (Exception e) {
            if (e instanceof SubstrateSdkException) {
                throw (SubstrateSdkException) e;
            }
            throw new SubstrateSdkException("Failed to restore Alibaba TableStore backup", e);
        }
    }

    @Override
    public void close() throws Exception {
        // HBR client doesn't need explicit closing in this SDK version
    }

    /**
     * Converts an Alibaba OTS snapshot to a Backup object.
     *
     * @param snapshot the OTS snapshot
     * @return the Backup object
     */
    private Backup convertToBackup(
            DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot) {

        BackupStatus status = convertSnapshotStatus(snapshot.getStatus());

        // Parse timestamp - CreatedTime is epoch timestamp in seconds (Long type)
        Instant creationTime = null;
        if (snapshot.getCreatedTime() != null) {
            creationTime = Instant.ofEpochSecond(snapshot.getCreatedTime());
        }

        // Calculate expiry time from retention period (Long type - seconds)
        Instant expiryTime = null;
        if (snapshot.getRetention() != null && creationTime != null) {
            expiryTime = creationTime.plusSeconds(snapshot.getRetention());
        }

        // Get actual bytes - ActualBytes is likely a String
        long sizeInBytes = -1L;
        if (snapshot.getActualBytes() != null) {
            try {
                sizeInBytes = Long.parseLong(snapshot.getActualBytes());
            } catch (NumberFormatException e) {
                // Ignore invalid size
            }
        }

        return Backup.builder()
                .backupId(snapshot.getSnapshotId())
                .resourceName(snapshot.getTableName() != null ? snapshot.getTableName() : getResourceName())
                .status(status)
                .creationTime(creationTime)
                .expiryTime(expiryTime)
                .sizeInBytes(sizeInBytes)
                .description(snapshot.getSnapshotId())
                .vaultId(snapshot.getVaultId())
                .build();
    }

    /**
     * Converts Alibaba snapshot status to BackupStatus.
     *
     * @param status the Alibaba snapshot status
     * @return the BackupStatus
     */
    private BackupStatus convertSnapshotStatus(String status) {
        if (status == null) {
            return BackupStatus.UNKNOWN;
        }

        switch (status.toUpperCase()) {
            case "COMPLETE":
                return BackupStatus.AVAILABLE;
            case "RUNNING":
                return BackupStatus.CREATING;
            case "FAILED":
            case "PARTIAL_COMPLETE":
                return BackupStatus.FAILED;
            default:
                return BackupStatus.UNKNOWN;
        }
    }

    /**
     * Builder for AliDBBackRestore.
     */
    public static class Builder extends AbstractDBBackRestore.Builder<AliDBBackRestore, Builder> {
        private Client hbrClient;
        private String accessKeyId;
        private String accessKeySecret;
        private String endpoint;

        /**
         * Default constructor.
         */
        public Builder() {
            this.providerId = "ali";
        }

        /**
         * Sets the Alibaba HBR client.
         *
         * @param hbrClient the HBR client
         * @return this builder
         */
        public Builder withHbrClient(Client hbrClient) {
            this.hbrClient = hbrClient;
            return this;
        }

        /**
         * Sets the Alibaba Cloud access key ID.
         *
         * @param accessKeyId the access key ID
         * @return this builder
         */
        public Builder withAccessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        /**
         * Sets the Alibaba Cloud access key secret.
         *
         * @param accessKeySecret the access key secret
         * @return this builder
         */
        public Builder withAccessKeySecret(String accessKeySecret) {
            this.accessKeySecret = accessKeySecret;
            return this;
        }

        /**
         * Sets the HBR service endpoint.
         *
         * @param endpoint the endpoint URL
         * @return this builder
         */
        public Builder withEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AliDBBackRestore build() {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("Region is required");
            }
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("Collection name is required");
            }

            // Create HBR client if not provided
            if (hbrClient == null && accessKeyId != null && accessKeySecret != null) {
                try {
                    Config config = new Config();
                    config.setAccessKeyId(accessKeyId);
                    config.setAccessKeySecret(accessKeySecret);

                    // Set endpoint based on region if not explicitly provided
                    if (endpoint != null && !endpoint.isEmpty()) {
                        config.setEndpoint(endpoint);
                    } else {
                        config.setEndpoint("hbr." + region + ".aliyuncs.com");
                    }

                    config.setRegionId(region);
                    hbrClient = new Client(config);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to create HBR client", e);
                }
            }

            return new AliDBBackRestore(this);
        }
    }
}
