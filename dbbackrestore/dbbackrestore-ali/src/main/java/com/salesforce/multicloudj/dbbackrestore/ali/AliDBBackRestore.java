package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyuncs.hbr.model.v20170908.CreateRestoreJobRequest;
import com.aliyuncs.hbr.model.v20170908.CreateRestoreJobResponse;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import java.util.List;

/**
 * Alibaba Cloud implementation of database backup and restore operations.
 * This implementation uses the HBR (Hybrid Backup Recovery) service for TableStore backups.
 *
 * <p>Note: Alibaba TableStore backups are managed through HBR service which requires
 * additional configuration such as vault ID and instance name. Some operations may
 * not be supported through the programmatic API and require the Alibaba Cloud Console.
 *
 * @since 0.2.26
 */
@AutoService(AbstractDBBackRestore.class)
public class AliDBBackRestore extends AbstractDBBackRestore {

  private final com.aliyuncs.IAcsClient hbrClient;

  /**
   * Default constructor for ServiceLoader.
   */
  public AliDBBackRestore() {
    super(new Builder());
    this.hbrClient = null;
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
    // Alibaba Cloud exception mapping
    return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
  }

  @Override
  public List<Backup> listBackups() {
    checkClosed();
    throw new UnSupportedOperationException(
        "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. "
            + "Listing backups requires HBR vault configuration. "
            + "Use Alibaba Cloud Console to view backups or configure HBR programmatically.");
  }

  @Override
  public Backup getBackup(String backupId) {
    checkClosed();
    throw new UnSupportedOperationException(
        "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. "
            + "Getting backup details requires HBR vault configuration. "
            + "Use Alibaba Cloud Console or configure HBR programmatically.");
  }

  @Override
  public BackupStatus getBackupStatus(String backupId) {
    checkClosed();
    throw new UnSupportedOperationException(
        "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. "
            + "Getting backup status requires HBR vault configuration. "
            + "Use Alibaba Cloud Console or configure HBR programmatically.");
  }

  @Override
  public void restoreBackup(RestoreRequest request) {
    checkClosed();
    if (hbrClient == null) {
      throw new SubstrateSdkException("HBR client not initialized. "
          + "Ensure credentials are configured in the builder.");
    }

    try {
      CreateRestoreJobRequest restoreRequest = new CreateRestoreJobRequest();
      restoreRequest.setRestoreType("OTS_TABLE");
      restoreRequest.setSnapshotId(request.getBackupId());

      // Set the target table name
      String targetTableName = request.getTargetCollectionName() != null
          && !request.getTargetCollectionName().isEmpty()
              ? request.getTargetCollectionName()
              : getCollectionName() + "-restored";

      // Note: Alibaba HBR restore for TableStore requires additional configuration
      // such as vault ID and instance name. These would need to be provided
      // through the RestoreRequest options.
      restoreRequest.setSourceType("OTS_TABLE");

      // Get additional options from request if provided
      if (request.getOptions() != null) {
        if (request.getOptions().containsKey("vaultId")) {
          restoreRequest.setVaultId(request.getOptions().get("vaultId"));
        }
        if (request.getOptions().containsKey("instanceName")) {
          // Additional HBR configuration can be added here
        }
      }

      CreateRestoreJobResponse response = hbrClient.getAcsResponse(restoreRequest);

      if (!response.getSuccess()) {
        throw new SubstrateSdkException("Failed to create restore job: "
            + response.getMessage() + " (Code: " + response.getCode() + ")");
      }
    } catch (Exception e) {
      if (e instanceof SubstrateSdkException) {
        throw (SubstrateSdkException) e;
      }
      throw new SubstrateSdkException("Failed to restore Alibaba TableStore backup", e);
    }
  }

  @Override
  public void deleteBackup(String backupId) {
    checkClosed();
    throw new UnSupportedOperationException(
        "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. "
            + "Deleting backups requires HBR vault configuration. "
            + "Use Alibaba Cloud Console or configure HBR programmatically.");
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      // HBR client doesn't need explicit closing
    }
  }

  /**
   * Builder for AliDBBackRestore.
   */
  public static class Builder extends AbstractDBBackRestore.Builder<AliDBBackRestore, Builder> {
    private com.aliyuncs.IAcsClient hbrClient;
    private String accessKeyId;
    private String accessKeySecret;

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
    public Builder withHbrClient(com.aliyuncs.IAcsClient hbrClient) {
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
        com.aliyuncs.profile.DefaultProfile profile = 
            com.aliyuncs.profile.DefaultProfile.getProfile(
                region, accessKeyId, accessKeySecret);
        hbrClient = new com.aliyuncs.DefaultAcsClient(profile);
      }

      return new AliDBBackRestore(this);
    }
  }
}
