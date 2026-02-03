package com.salesforce.multicloudj.dbbackrestore.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse;
import software.amazon.awssdk.services.backup.model.RecoveryPointByResource;
import software.amazon.awssdk.services.backup.model.RecoveryPointStatus;
import software.amazon.awssdk.services.backup.model.StartRestoreJobRequest;

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
@AutoService(AbstractDBBackRestore.class)
public class AwsDBBackRestore extends AbstractDBBackRestore {

    private final BackupClient backupClient;
    private final String tableArn;

    /**
     * Default constructor for ServiceLoader.
     */
    public AwsDBBackRestore() {
        super(new Builder());
        this.backupClient = null;
        this.tableArn = null;
    }

    /**
     * Constructs an AwsDBBackRestore with the given builder.
     *
     * @param builder the builder containing configuration
     */
    public AwsDBBackRestore(Builder builder) {
        super(builder);
        this.backupClient = builder.backupClient;

        // Build the DynamoDB table ARN
        // Format: arn:aws:dynamodb:{region}:{account-id}:table/{table-name}
        // Since we don't have account ID readily available, we'll construct a partial ARN
        // that AWS Backup API can work with, or get it from the table description
        this.tableArn = builder.tableArn != null ? builder.tableArn
                : String.format("arn:aws:dynamodb:%s:*:table/%s", builder.getRegion(), builder.getResourceName());
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
        ListRecoveryPointsByResourceRequest request =
                ListRecoveryPointsByResourceRequest.builder()
                        .resourceArn(tableArn)
                        .build();

        ListRecoveryPointsByResourceResponse response =
                backupClient.listRecoveryPointsByResource(request);

        List<Backup> backups = new ArrayList<>();
        for (RecoveryPointByResource recoveryPoint : response.recoveryPoints()) {
            backups.add(convertToBackup(recoveryPoint));
        }

        return backups;
    }

    @Override
    public Backup getBackup(String backupId) {
        // First, get the vault name by listing recovery points for this resource
        String vaultName = getVaultNameForRecoveryPoint(backupId);

        DescribeRecoveryPointRequest request = DescribeRecoveryPointRequest.builder()
                .backupVaultName(vaultName)
                .recoveryPointArn(backupId)
                .build();

        DescribeRecoveryPointResponse response = backupClient.describeRecoveryPoint(request);
        return convertToBackup(response);
    }

    @Override
    public BackupStatus getBackupStatus(String backupId) {
        // First, get the vault name by listing recovery points for this resource
        String vaultName = getVaultNameForRecoveryPoint(backupId);

        DescribeRecoveryPointRequest request =
                DescribeRecoveryPointRequest.builder()
                        .backupVaultName(vaultName)
                        .recoveryPointArn(backupId)
                        .build();

        DescribeRecoveryPointResponse response =
                backupClient.describeRecoveryPoint(request);
        return convertRecoveryPointStatus(response.status());
    }

    @Override
    public void restoreBackup(RestoreRequest request) {
        // The role is assumed by AWS Backup to create the restored table.
        String iamRoleArn = request.getRoleId();
        if (StringUtils.isBlank(iamRoleArn)) {
            throw new IllegalArgumentException("Role ID cannot be null or empty for AWS");
        }

        String targetTableName =  request.getTargetResource();
        if (StringUtils.isBlank(targetTableName)) {
            throw new IllegalArgumentException("target resource cannot be null or empty");
        }

        Map<String, String> metadata = new HashMap<>();
        metadata.put("targetTableName", targetTableName);

        StartRestoreJobRequest restoreJobRequest = StartRestoreJobRequest.builder()
                .recoveryPointArn(request.getBackupId())
                .metadata(metadata)
                .idempotencyToken(UUID.uniqueString())
                .iamRoleArn(iamRoleArn)
                .resourceType("DynamoDB")
                .build();

        backupClient.startRestoreJob(restoreJobRequest);
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
        ListRecoveryPointsByResourceRequest request =
                ListRecoveryPointsByResourceRequest.builder()
                        .resourceArn(tableArn)
                        .build();

        ListRecoveryPointsByResourceResponse response =
                backupClient.listRecoveryPointsByResource(request);

        for (RecoveryPointByResource recoveryPoint : response.recoveryPoints()) {
            if (recoveryPoint.recoveryPointArn().equals(recoveryPointArn)) {
                return recoveryPoint.backupVaultName();
            }
        }

        throw new ResourceNotFoundException("Recovery point not found: " + recoveryPointArn);
    }

    private Backup convertToBackup(
            RecoveryPointByResource recoveryPoint) {
        return Backup.builder()
                .backupId(recoveryPoint.recoveryPointArn())
                .resourceName(getResourceName())
                .status(convertRecoveryPointStatus(recoveryPoint.status()))
                .creationTime(recoveryPoint.creationDate())
                .expiryTime(null)
                .sizeInBytes(recoveryPoint.backupSizeBytes())
                .vaultId(recoveryPoint.backupVaultName())
                .build();
    }

    private Backup convertToBackup(DescribeRecoveryPointResponse response) {
        return Backup.builder()
                .backupId(response.recoveryPointArn())
                .resourceName(getResourceName())
                .status(convertRecoveryPointStatus(response.status()))
                .creationTime(response.creationDate())
                .expiryTime(response.calculatedLifecycle() != null
                        ? response.calculatedLifecycle().deleteAt() : null)
                .sizeInBytes(response.backupSizeInBytes())
                .vaultId(response.backupVaultName())
                .build();
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

    /**
     * Builder for AwsDBBackRestore.
     */
    public static class Builder extends AbstractDBBackRestore.Builder<AwsDBBackRestore, Builder> {
        private software.amazon.awssdk.services.backup.BackupClient backupClient;
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

        /**
         * Sets the DynamoDB table ARN.
         *
         * @param tableArn the table ARN
         * @return this builder
         */
        public Builder withTableArn(String tableArn) {
            this.tableArn = tableArn;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AwsDBBackRestore build() {
            if (StringUtils.isBlank(region)) {
                throw new IllegalArgumentException("Region is required");
            }
            if (StringUtils.isBlank(resourceName)) {
                throw new IllegalArgumentException("Collection name is required");
            }

            // Create backup client if not provided
            if (backupClient == null) {
                backupClient = BackupClient.builder().region(Region.of(region)).build();
            }

            return new AwsDBBackRestore(this);
        }
    }
}
