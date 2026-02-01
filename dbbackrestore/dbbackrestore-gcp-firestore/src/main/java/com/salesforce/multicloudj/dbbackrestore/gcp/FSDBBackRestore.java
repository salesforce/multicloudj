package com.salesforce.multicloudj.dbbackrestore.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.firestore.admin.v1.ListBackupsResponse;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
import java.io.IOException;
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

  private final FirestoreAdminClient firestoreAdminClient;
  private final String projectId;

  /**
   * Default constructor for ServiceLoader.
   */
  public FSDBBackRestore() {
    super(new Builder());
    this.firestoreAdminClient = null;
    this.projectId = null;
  }

  /**
   * Constructs a FSDBBackRestore with the given builder.
   *
   * @param builder the builder containing configuration
   */
  public FSDBBackRestore(Builder builder) {
    super(builder);
    this.firestoreAdminClient = builder.firestoreAdminClient;
    this.projectId = builder.projectId;
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
          return com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException.class;
        case ALREADY_EXISTS:
          return com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException.class;
        case PERMISSION_DENIED:
        case UNAUTHENTICATED:
          return com.salesforce.multicloudj.common.exceptions.UnAuthorizedException.class;
        default:
          return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
      }
    }
    return com.salesforce.multicloudj.common.exceptions.UnknownException.class;
  }

  @Override
  public List<Backup> listBackups() {
    if (firestoreAdminClient == null) {
      throw new SubstrateSdkException("FirestoreAdminClient not initialized. " 
          + "Ensure credentials are configured in the builder.");
    }

    try {
      // Firestore backups are at location level, not collection-specific
      // Format: projects/{project}/locations/{location}
      String locationName = extractLocationName();

      ListBackupsResponse response = firestoreAdminClient.listBackups(locationName);

      List<Backup> backups = new ArrayList<>();
      for (com.google.firestore.admin.v1.Backup backup : response.getBackupsList()) {
        backups.add(convertToBackup(backup));
      }

      return backups;
    } catch (ApiException e) {
      throw new SubstrateSdkException("Failed to list Firestore backups", e);
    }
  }

  @Override
  public Backup getBackup(String backupId) {
    if (firestoreAdminClient == null) {
      throw new SubstrateSdkException("FirestoreAdminClient not initialized. " 
          + "Ensure credentials are configured in the builder.");
    }

    try {
      com.google.firestore.admin.v1.Backup backup = firestoreAdminClient.getBackup(backupId);
      return convertToBackup(backup);
    } catch (ApiException e) {
      throw new SubstrateSdkException("Failed to get Firestore backup: " + backupId, e);
    }
  }

  @Override
  public BackupStatus getBackupStatus(String backupId) {
    Backup backup = getBackup(backupId);
    return backup.getStatus();
  }

  @Override
  public void restoreBackup(RestoreRequest request) {
    if (firestoreAdminClient == null) {
      throw new SubstrateSdkException("FirestoreAdminClient not initialized. " 
          + "Ensure credentials are configured in the builder.");
    }

    try {
      // Build restore request
      String targetDatabaseId = request.getTargetTable() != null
          && !request.getTargetTable().isEmpty()
              ? request.getTargetTable()
              : getCollectionName() + "-restored";

      // Extract parent from backup ID (format: projects/{project}/locations/{location}/backups/{backup})
      String parent = request.getBackupId().substring(
          0, request.getBackupId().lastIndexOf("/backups/"));
      parent = parent.replace("/locations/", "/");

      com.google.firestore.admin.v1.RestoreDatabaseRequest.Builder restoreBuilder =
          com.google.firestore.admin.v1.RestoreDatabaseRequest.newBuilder()
              .setParent(parent)
              .setDatabaseId(targetDatabaseId)
              .setBackup(request.getBackupId());

      // Restore is a long-running operation
      firestoreAdminClient.restoreDatabaseAsync(restoreBuilder.build());
    } catch (Exception e) {
      throw new SubstrateSdkException("Failed to restore Firestore backup", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (firestoreAdminClient != null) {
      firestoreAdminClient.close();
    }
  }

  /**
   * Extracts location name from the collection name for backup operations.
   * Format: projects/{project}/locations/{location}
   */
  private String extractLocationName() {
    // Collection name format: projects/{project}/databases/{database}/documents/{collection}
    String collectionName = getCollectionName();
    String[] parts = collectionName.split("/");
    if (parts.length >= 2 && parts[0].equals("projects")) {
      // Default to 'nam5' if region not explicitly specified
      return "projects/" + parts[1] + "/locations/nam5";
    }
    throw new SubstrateSdkException(
        "Invalid collection name format. Expected format: projects/{project}/... but got: " 
        + collectionName);
  }

  /**
   * Converts Firestore Admin API Backup to MultiCloudJ Backup model.
   */
  private Backup convertToBackup(com.google.firestore.admin.v1.Backup backup) {
    return Backup.builder()
        .backupId(backup.getName())
        .collectionName(backup.getDatabase())
        .status(convertBackupState(backup.getState()))
        .creationTime(java.time.Instant.ofEpochSecond(
            backup.getSnapshotTime().getSeconds(),
            backup.getSnapshotTime().getNanos()))
        .expiryTime(java.time.Instant.ofEpochSecond(
            backup.getExpireTime().getSeconds(),
            backup.getExpireTime().getNanos()))
        .sizeInBytes(-1) // Size not available in Backup object
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
    private String projectId;

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

    /**
     * Sets the Google credentials for authentication.
     *
     * @param credentials the Google credentials
     * @return this builder
     */
    public Builder withCredentials(GoogleCredentials credentials) {
      this.credentials = credentials;
      return this;
    }

    /**
     * Sets the GCP project ID.
     *
     * @param projectId the project ID
     * @return this builder
     */
    public Builder withProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public FSDBBackRestore build() {
      if (region == null || region.isEmpty()) {
        throw new IllegalArgumentException("Region is required");
      }
      if (collectionName == null || collectionName.isEmpty()) {
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
