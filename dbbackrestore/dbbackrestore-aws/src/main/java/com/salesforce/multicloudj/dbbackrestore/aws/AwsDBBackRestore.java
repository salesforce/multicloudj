package com.salesforce.multicloudj.dbbackrestore.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * AWS implementation of database backup and restore operations using AWS Backup service.
 * This implementation works with DynamoDB tables.
 *
 * @since 0.2.26
 */
@AutoService(AbstractDBBackRestore.class)
public class AwsDBBackRestore extends AbstractDBBackRestore {

  private final software.amazon.awssdk.services.backup.BackupClient backupClient;
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
        : String.format("arn:aws:dynamodb:%s:*:table/%s", builder.getRegion(), builder.getCollectionName());
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
    checkClosed();
    software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest request =
        software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest.builder()
            .resourceArn(tableArn)
            .build();

    software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse response =
        backupClient.listRecoveryPointsByResource(request);

    List<Backup> backups = new ArrayList<>();
    for (software.amazon.awssdk.services.backup.model.RecoveryPointByResource recoveryPoint 
        : response.recoveryPoints()) {
      backups.add(convertToBackup(recoveryPoint));
    }

    return backups;
  }

  @Override
  public Backup getBackup(String backupId) {
    checkClosed();
    // First, get the vault name by listing recovery points for this resource
    String vaultName = getVaultNameForRecoveryPoint(backupId);

    software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest request =
        software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest.builder()
            .backupVaultName(vaultName)
            .recoveryPointArn(backupId)
            .build();

    software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse response =
        backupClient.describeRecoveryPoint(request);
    return convertToBackup(response);
  }

  @Override
  public BackupStatus getBackupStatus(String backupId) {
    checkClosed();
    // First, get the vault name by listing recovery points for this resource
    String vaultName = getVaultNameForRecoveryPoint(backupId);

    software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest request =
        software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest.builder()
            .backupVaultName(vaultName)
            .recoveryPointArn(backupId)
            .build();

    software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse response =
        backupClient.describeRecoveryPoint(request);
    return convertRecoveryPointStatus(response.status());
  }

  @Override
  public void restoreBackup(RestoreRequest request) {
    checkClosed();
    // AWS Backup requires an IAM role ARN for DynamoDB restore (required for Advanced DynamoDB and
    // recommended for all DynamoDB restores). The role is assumed by AWS Backup to create the restored table.
    String iamRoleArn = null;
    if (request.getOptions() != null && request.getOptions().get("iamRoleArn") != null) {
      iamRoleArn = request.getOptions().get("iamRoleArn").trim();
    }
    if (iamRoleArn == null || iamRoleArn.isEmpty()) {
      throw new SubstrateSdkException(
          "IAM role ARN is required for AWS Backup DynamoDB restore. "
              + "Provide it via RestoreRequest.options with key \"iamRoleArn\" (e.g. "
              + "RestoreRequest.builder().backupId(...).options(Map.of(\"iamRoleArn\", "
              + "\"arn:aws:iam::123456789012:role/YourBackupRestoreRole\")).build()). "
              + "The IAM role must have permissions for dynamodb:RestoreTableFromAwsBackup and "
              + "trust policy allowing AWS Backup to assume it.");
    }

    String targetTableName = request.getTargetCollectionName() != null 
        && !request.getTargetCollectionName().isEmpty()
            ? request.getTargetCollectionName()
            : getCollectionName();

    // Build restore metadata for DynamoDB table
    java.util.Map<String, String> metadata = new java.util.HashMap<>();
    metadata.put("targetTableName", targetTableName);

    software.amazon.awssdk.services.backup.model.StartRestoreJobRequest restoreJobRequest =
        software.amazon.awssdk.services.backup.model.StartRestoreJobRequest.builder()
            .recoveryPointArn(request.getBackupId())
            .metadata(metadata)
            .iamRoleArn(iamRoleArn)
            .resourceType("DynamoDB")
            .build();

    backupClient.startRestoreJob(restoreJobRequest);
  }

  @Override
  public void deleteBackup(String backupId) {
    checkClosed();
    // First, get the vault name by listing recovery points for this resource
    String vaultName = getVaultNameForRecoveryPoint(backupId);

    software.amazon.awssdk.services.backup.model.DeleteRecoveryPointRequest request =
        software.amazon.awssdk.services.backup.model.DeleteRecoveryPointRequest.builder()
            .backupVaultName(vaultName)
            .recoveryPointArn(backupId)
            .build();

    backupClient.deleteRecoveryPoint(request);
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      if (backupClient != null) {
        backupClient.close();
      }
    }
  }

  /**
   * Gets the vault name for a recovery point by listing recovery points for this resource.
   * AWS Backup requires both vault name and recovery point ARN, but the ARN alone doesn't contain vault info.
   */
  private String getVaultNameForRecoveryPoint(String recoveryPointArn) {
    software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest request =
        software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest.builder()
            .resourceArn(tableArn)
            .build();

    software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse response =
        backupClient.listRecoveryPointsByResource(request);

    for (software.amazon.awssdk.services.backup.model.RecoveryPointByResource recoveryPoint 
        : response.recoveryPoints()) {
      if (recoveryPoint.recoveryPointArn().equals(recoveryPointArn)) {
        return recoveryPoint.backupVaultName();
      }
    }

    throw new ResourceNotFoundException("Recovery point not found: " + recoveryPointArn);
  }

  private Backup convertToBackup(
      software.amazon.awssdk.services.backup.model.RecoveryPointByResource recoveryPoint) {
    java.util.Map<String, String> metadata = new java.util.HashMap<>();
    metadata.put("backupVaultName", recoveryPoint.backupVaultName());

    return Backup.builder()
        .backupId(recoveryPoint.recoveryPointArn())
        .collectionName(getCollectionName())
        .status(convertRecoveryPointStatus(recoveryPoint.status()))
        .creationTime(recoveryPoint.creationDate())
        .expiryTime(null) // Not available in RecoveryPointByResource
        .sizeInBytes(-1L) // Not available in RecoveryPointByResource
        .metadata(metadata)
        .build();
  }

  private Backup convertToBackup(
      software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse response) {
    return Backup.builder()
        .backupId(response.recoveryPointArn())
        .collectionName(getCollectionName())
        .status(convertRecoveryPointStatus(response.status()))
        .creationTime(response.creationDate())
        .expiryTime(response.calculatedLifecycle() != null 
            ? response.calculatedLifecycle().deleteAt() : null)
        .sizeInBytes(response.backupSizeInBytes() != null ? response.backupSizeInBytes() : -1L)
        .build();
  }

  private BackupStatus convertRecoveryPointStatus(
      software.amazon.awssdk.services.backup.model.RecoveryPointStatus status) {
    if (status == null) {
      return BackupStatus.UNKNOWN;
    }
    switch (status) {
      case PARTIAL:
        return BackupStatus.CREATING;
      case COMPLETED:
        return BackupStatus.AVAILABLE;
      case DELETING:
        return BackupStatus.DELETING;
      case EXPIRED:
        return BackupStatus.DELETED;
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
      this.providerId = "aws";
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
      if (region == null || region.isEmpty()) {
        throw new IllegalArgumentException("Region is required");
      }
      if (collectionName == null || collectionName.isEmpty()) {
        throw new IllegalArgumentException("Collection name is required");
      }

      // Create backup client if not provided
      if (backupClient == null) {
        AwsCredentialsProvider credentialsProvider = 
            CredentialsProvider.getCredentialsProvider(null, software.amazon.awssdk.regions.Region.of(region));
        backupClient = software.amazon.awssdk.services.backup.BackupClient.builder()
            .region(software.amazon.awssdk.regions.Region.of(region))
            .credentialsProvider(credentialsProvider)
            .build();
      }

      return new AwsDBBackRestore(this);
    }
  }
}
