package com.salesforce.multicloudj.dbbackuprestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.hbr20170908.models.CreateRestoreJobRequest;
import com.aliyun.hbr20170908.models.CreateRestoreJobResponse;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsRequest;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponse;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponseBody;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2Request;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2Response;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2ResponseBody;
import com.aliyun.hbr20170908.models.OtsTableRestoreDetail;
import com.aliyun.teaopenapi.models.Config;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreStatus;

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
@AutoService(AbstractDBBackupRestore.class)
public class AliDBBackupRestore extends AbstractDBBackupRestore {

    private Client hbrClient;

    /**
     * Default constructor for ServiceLoader.
     */
    public AliDBBackupRestore() {
        super(new Builder());
    }

    /**
     * Constructs an AliDBBackupRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public AliDBBackupRestore(Builder builder) {
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
    public String restoreBackup(RestoreRequest request) {
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
            CreateRestoreJobResponse response = hbrClient.createRestoreJob(restoreJobRequest);
            // Return the restore job ID
            return response.getBody().getRestoreId();
        } catch (Exception e) {
            throw new UnknownException("failed to create restore job", e);
        }
    }

    @Override
    public Restore getRestoreJob(String restoreId) {
        DescribeRestoreJobs2Request request = new DescribeRestoreJobs2Request();

        // Use filters to query for the specific restore job
        DescribeRestoreJobs2Request.DescribeRestoreJobs2RequestFilters filter =
                new DescribeRestoreJobs2Request.DescribeRestoreJobs2RequestFilters();
        filter.setKey("JobId");
        filter.setOperator("EQUAL");
        List<String> values = new ArrayList<>();
        values.add(restoreId);
        filter.setValues(values);

        List<DescribeRestoreJobs2Request.DescribeRestoreJobs2RequestFilters> filters =
                new java.util.ArrayList<>();
        filters.add(filter);
        request.setFilters(filters);

        try {
            DescribeRestoreJobs2Response response = hbrClient.describeRestoreJobs2(request);

            if (response.getBody() == null || response.getBody().getRestoreJobs() == null
                    || response.getBody().getRestoreJobs().getRestoreJob().isEmpty()) {
                throw new ResourceNotFoundException("Restore job not found: " + restoreId);
            }

            DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobsRestoreJob restoreJob =
                    response.getBody().getRestoreJobs().getRestoreJob().get(0);
            return convertToRestore(restoreJob);
        } catch (Exception e) {
            if (e instanceof ResourceNotFoundException) {
                throw (ResourceNotFoundException) e;
            }
            throw new UnknownException("failed to describe restore job", e);
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
     * @param status the Alibaba snapshot status, valid statuses are here
     *               <a href="https://www.alibabacloud.com/help/en/cloud-backup/developer-reference/api-hbr-2017-09-08-describebackupjobs2">...</a>
     * @return the BackupStatus
     */
    private BackupStatus convertSnapshotStatus(String status) {
        if (status == null) {
            return BackupStatus.UNKNOWN;
        }

        switch (status.toUpperCase()) {
            case "COMPLETE":
                return BackupStatus.AVAILABLE;
            case "PARTIAL_COMPLETE":
                return BackupStatus.CREATING;
            case "FAILED":
                return BackupStatus.FAILED;
            default:
                return BackupStatus.UNKNOWN;
        }
    }

    /**
     * Converts Alibaba restore job to Restore model.
     *
     * @param restoreJob the Alibaba restore job
     * @return the Restore object
     */
    private Restore convertToRestore(DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobsRestoreJob restoreJob) {

        RestoreStatus status = convertRestoreJobStatus(restoreJob.getStatus());

        // Parse timestamps - CreatedTime and CompleteTime are epoch timestamps in seconds (Long type)
        Instant startTime = null;
        if (restoreJob.getCreatedTime() != null) {
            startTime = Instant.ofEpochSecond(restoreJob.getCreatedTime());
        }

        Instant endTime = null;
        if (restoreJob.getCompleteTime() != null) {
            endTime = Instant.ofEpochSecond(restoreJob.getCompleteTime());
        }

        return Restore.builder()
                .restoreId(restoreJob.getRestoreId())
                .backupId(restoreJob.getSnapshotId())
                .targetResource(restoreJob.getTargetTableName())
                .status(status)
                .startTime(startTime)
                .endTime(endTime)
                .statusMessage(restoreJob.getErrorMessage())
                .build();
    }

    /**
     * Converts Alibaba restore job status to RestoreStatus.
     *
     * @param status the Alibaba restore job status
     * @return the RestoreStatus
     */
    private RestoreStatus convertRestoreJobStatus(String status) {
        if (status == null) {
            return RestoreStatus.UNKNOWN;
        }

        switch (status.toUpperCase()) {
            case "COMPLETE":
                return RestoreStatus.COMPLETED;
            case "RUNNING":
            case "PARTIAL_COMPLETE":
                return RestoreStatus.RESTORING;
            case "FAILED":
            case "EXPIRED":
            case "CANCELED":
                return RestoreStatus.FAILED;
            default:
                return RestoreStatus.UNKNOWN;
        }
    }

    /**
     * Builder for AliDBBackupRestore.
     */
    public static class Builder extends AbstractDBBackupRestore.Builder<AliDBBackupRestore, Builder> {
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
        public com.salesforce.multicloudj.dbbackuprestore.ali.AliDBBackupRestore build() {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("Region is required");
            }
            if (resourceName == null || resourceName.isEmpty()) {
                throw new IllegalArgumentException("table name is required");
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

            return new com.salesforce.multicloudj.dbbackuprestore.ali.AliDBBackupRestore(this);
        }
    }
}
