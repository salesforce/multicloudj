package com.salesforce.multicloudj.dbbackuprestore.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreStatus;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse;
import software.amazon.awssdk.services.backup.model.DescribeRestoreJobRequest;
import software.amazon.awssdk.services.backup.model.DescribeRestoreJobResponse;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse;
import software.amazon.awssdk.services.backup.model.RecoveryPointByResource;
import software.amazon.awssdk.services.backup.model.RecoveryPointStatus;
import software.amazon.awssdk.services.backup.model.RestoreJobStatus;
import software.amazon.awssdk.services.backup.model.StartRestoreJobRequest;
import software.amazon.awssdk.services.backup.model.StartRestoreJobResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AWS implementation of database backup and restore operations using AWS Backup service.
 * This implementation works with DynamoDB tables.
 *
 * @since 0.2.25
 */
@AutoService(AbstractDBBackupRestore.class)
public class AwsDBBackupRestore extends AbstractDBBackupRestore {

    private final BackupClient backupClient;
    private final String tableArn;

    /**
     * Default constructor for ServiceLoader.
     */
    public AwsDBBackupRestore() {
        super(new Builder());
        this.backupClient = null;
        this.tableArn = null;
    }

    /**
     * Constructs an AwsDBBackupRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public AwsDBBackupRestore(Builder builder) {
        super(builder);
        this.backupClient = builder.backupClient;
        this.tableArn = builder.getResourceName();
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
        ListRecoveryPointsByResourceRequest request = ListRecoveryPointsByResourceRequest.builder().resourceArn(tableArn).build();

        ListRecoveryPointsByResourceResponse response = backupClient.listRecoveryPointsByResource(request);

        List<Backup> backups = new ArrayList<>();
        for (RecoveryPointByResource recoveryPoint : response.recoveryPoints()) {
            backups.add(convertToBackup(recoveryPoint));
        }

        return backups;
    }

    @Override
    public Backup getBackup(String backupId) {
        DescribeRecoveryPointResponse response = describeRecoveryPoint(backupId);
        return convertToBackup(response);
    }

    private DescribeRecoveryPointResponse describeRecoveryPoint(String backupId) {
        // First, get the vault name by listing recovery points for this resource
        String vaultName = getVaultNameForRecoveryPoint(backupId);

        DescribeRecoveryPointRequest request = DescribeRecoveryPointRequest.builder().backupVaultName(vaultName).recoveryPointArn(backupId).build();

        return backupClient.describeRecoveryPoint(request);
    }

    @Override
    public String restoreBackup(RestoreRequest request) {
        // The role is assumed by AWS Backup to create the restored table.
        String iamRoleArn = request.getRoleId();
        if (StringUtils.isBlank(iamRoleArn)) {
            throw new IllegalArgumentException("Role ID cannot be null or empty for AWS");
        }

        String targetTableName = request.getTargetResource();
        if (StringUtils.isBlank(targetTableName)) {
            throw new IllegalArgumentException("target resource cannot be null or empty");
        }

        Map<String, String> metadata = new HashMap<>();
        metadata.put("targetTableName", targetTableName);

        if (StringUtils.isNotBlank(request.getKmsEncryptionKeyId())) {
            metadata.put("encryptionType", "KMS");
            metadata.put("kmsMasterKeyArn", request.getKmsEncryptionKeyId());
        }

        StartRestoreJobRequest restoreJobRequest = StartRestoreJobRequest.builder().recoveryPointArn(request.getBackupId()).metadata(metadata).idempotencyToken(UUID.uniqueString()).iamRoleArn(iamRoleArn).resourceType("DynamoDB").build();

        StartRestoreJobResponse response = backupClient.startRestoreJob(restoreJobRequest);

        // Return the restore job ARN as the restore ID
        return response.restoreJobId();
    }

    @Override
    public Restore getRestoreJob(String restoreId) {
        DescribeRestoreJobRequest request = DescribeRestoreJobRequest.builder()
                .restoreJobId(restoreId)
                .build();

        DescribeRestoreJobResponse response = backupClient.describeRestoreJob(request);
        return convertToRestore(response);
    }

    @Override
    public void close() {
        if (backupClient != null) {
            backupClient.close();
        }
    }

    /**
     * Gets the vault name for a recovery point by listing recovery points for this resource.
     * AWS Backup requires both vault name and recovery point ARN, but the ARN alone doesn't contain vault info.
     */
    private String getVaultNameForRecoveryPoint(String recoveryPointArn) {
        ListRecoveryPointsByResourceRequest request = ListRecoveryPointsByResourceRequest.builder().resourceArn(tableArn).build();

        ListRecoveryPointsByResourceResponse response = backupClient.listRecoveryPointsByResource(request);

        for (RecoveryPointByResource recoveryPoint : response.recoveryPoints()) {
            if (recoveryPoint.recoveryPointArn().equals(recoveryPointArn)) {
                return recoveryPoint.backupVaultName();
            }
        }

        throw new ResourceNotFoundException("Recovery point not found: " + recoveryPointArn);
    }

    private Backup convertToBackup(RecoveryPointByResource recoveryPoint) {
        return Backup.builder().backupId(recoveryPoint.recoveryPointArn()).resourceName(getResourceName()).status(convertRecoveryPointStatus(recoveryPoint.status())).creationTime(recoveryPoint.creationDate()).expiryTime(null).sizeInBytes(recoveryPoint.backupSizeBytes()).vaultId(recoveryPoint.backupVaultName()).build();
    }

    private Backup convertToBackup(DescribeRecoveryPointResponse response) {
        return Backup.builder().backupId(response.recoveryPointArn()).resourceName(getResourceName()).status(convertRecoveryPointStatus(response.status())).creationTime(response.creationDate()).expiryTime(response.calculatedLifecycle() != null ? response.calculatedLifecycle().deleteAt() : null).sizeInBytes(response.backupSizeInBytes()).vaultId(response.backupVaultName()).build();
    }

    private BackupStatus convertRecoveryPointStatus(RecoveryPointStatus status) {
        if (status == null) {
            return BackupStatus.UNKNOWN;
        }
        switch (status) {
            case PARTIAL:
            case CREATING:
                return BackupStatus.CREATING;
            case COMPLETED:
            case AVAILABLE:
                return BackupStatus.AVAILABLE;
            case DELETING:
                return BackupStatus.DELETING;
            case EXPIRED:
                return BackupStatus.DELETED;
            case STOPPED:
                return BackupStatus.FAILED;
            default:
                return BackupStatus.UNKNOWN;
        }
    }

    private Restore convertToRestore(DescribeRestoreJobResponse response) {
        return Restore.builder()
                .restoreId(response.restoreJobId())
                .backupId(response.recoveryPointArn())
                .targetResource(response.createdResourceArn())
                .status(convertRestoreJobStatus(response.status()))
                .startTime(response.creationDate())
                .endTime(response.completionDate())
                .statusMessage(response.statusMessage())
                .build();
    }

    private RestoreStatus convertRestoreJobStatus(RestoreJobStatus status) {
        if (status == null) {
            return RestoreStatus.UNKNOWN;
        }
        switch (status) {
            case PENDING:
            case RUNNING:
                return RestoreStatus.RESTORING;
            case COMPLETED:
                return RestoreStatus.COMPLETED;
            case ABORTED:
            case FAILED:
                return RestoreStatus.FAILED;
            default:
                return RestoreStatus.UNKNOWN;
        }
    }

    /**
     * Builder for AwsDBBackupRestore.
     */
    public static class Builder extends AbstractDBBackupRestore.Builder<AwsDBBackupRestore, Builder> {
        private BackupClient backupClient;
        private String tableArn;

        /**
         * Default constructor.
         */
        public Builder() {
            this.providerId = AwsConstants.PROVIDER_ID;
        }

        /**
         * Sets the AWS Backup client.
         *
         * @param backupClient the backup client
         * @return this builder
         */
        public Builder withBackupClient(software.amazon.awssdk.services.backup.BackupClient backupClient) {
            this.backupClient = backupClient;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AwsDBBackupRestore build() {
            if (StringUtils.isBlank(region)) {
                throw new IllegalArgumentException("Region is required");
            }
            if (StringUtils.isBlank(resourceName)) {
                throw new IllegalArgumentException("Table ARN is required");
            }

            // Create backup client if not provided
            if (backupClient == null) {
                backupClient = BackupClient.builder().region(Region.of(region)).build();
            }

            return new AwsDBBackupRestore(this);
        }
    }
}
