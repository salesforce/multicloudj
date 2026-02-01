package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.hbr20170908.models.CreateRestoreJobRequest;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsRequest;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponse;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponseBody;
import com.aliyun.hbr20170908.models.OtsTableRestoreDetail;
import com.aliyun.teaopenapi.models.Config;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
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

        DescribeOtsTableSnapshotsRequest request = new DescribeOtsTableSnapshotsRequest();
        DescribeOtsTableSnapshotsResponse response = null;
        try {
            response = hbrClient.describeOtsTableSnapshots(request);
        } catch (Exception e) {
            throw new UnknownException("failed to describe OTS table snapshots", e);
        }

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

    }

    @Override
    public Backup getBackup(String backupId) {

        // List all snapshots and find the matching one
        List<Backup> backups = listBackups();

        for (Backup backup : backups) {
            if (backup.getBackupId().equals(backupId)) {
                return backup;
            }
        }

        throw new ResourceNotFoundException("Backup not found: " + backupId);

    }

    @Override
    public BackupStatus getBackupStatus(String backupId) {
        Backup backup = getBackup(backupId);
        return backup.getStatus();
    }

    @Override
    public void restoreBackup(RestoreRequest request) {
        CreateRestoreJobRequest restoreJobRequest = new CreateRestoreJobRequest();
        restoreJobRequest.setRestoreType("OTS_TABLE");
        restoreJobRequest.setSnapshotId(request.getBackupId());
        
        // Configure OTS restore details
        OtsTableRestoreDetail otsDetail = new OtsTableRestoreDetail();
        otsDetail.setOverwriteExisting(false);
        restoreJobRequest.setOtsDetail(otsDetail);

        // Set vault ID if provided (required for restore)
        if (request.getVaultId() != null && !request.getVaultId().isEmpty()) {
            restoreJobRequest.setVaultId(request.getVaultId());
        }

        try {
            hbrClient.createRestoreJob(restoreJobRequest);
        } catch (Exception e) {
            throw new UnknownException("failed to create restore job", e);
        }
    }

    @Override
    public void close() {
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
            sizeInBytes = Long.parseLong(snapshot.getActualBytes());
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

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AliDBBackRestore build() {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("Region is required");
            }
            if (resourceName == null || resourceName.isEmpty()) {
                throw new IllegalArgumentException("Collection name is required");
            }

            // Create HBR client if not provided
            if (hbrClient == null) {
                try {
                    Config config = new Config();
                    config.setRegionId(region);
                    hbrClient = new Client(config);
                } catch (Exception e) {
                    throw new UnknownException("Failed to create HBR client", e);
                }
            }

            return new AliDBBackRestore(this);
        }
    }
}
