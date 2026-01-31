package com.salesforce.multicloudj.dbbackrestore.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import java.util.List;
import lombok.Getter;

/**
 * Base class for database backup and restore implementations.
 * This abstract class provides the contract for backup and restore operations
 * across different cloud providers.
 *
 * @since 0.2.26
 */
public abstract class AbstractDBBackRestore implements Provider, AutoCloseable {

  private final String providerId;
  @Getter
  protected final String region;
  @Getter
  protected final String collectionName;

  protected boolean closed = false;

  /**
   * Constructs an AbstractDBBackRestore instance using a Builder.
   *
   * @param builder the Builder instance to use for construction
   */
  protected AbstractDBBackRestore(Builder<?, ?> builder) {
    this(builder.providerId, builder.region, builder.collectionName);
  }

  /**
   * Constructs an AbstractDBBackRestore instance.
   *
   * @param providerId the cloud provider identifier (e.g., "aws", "gcp-firestore", "ali")
   * @param region the region where the database is located
   * @param collectionName the name of the collection/table to backup/restore
   */
  protected AbstractDBBackRestore(String providerId, String region, String collectionName) {
    this.providerId = providerId;
    this.region = region;
    this.collectionName = collectionName;
  }

  @Override
  public String getProviderId() {
    return providerId;
  }

  /**
   * Lists all available backups for the collection/table.
   *
   * @return a list of Backup objects representing available backups
   * @since 0.2.26
   */
  public abstract List<Backup> listBackups();

  /**
   * Gets details of a specific backup by its ID.
   *
   * @param backupId the unique identifier of the backup
   * @return the Backup object with full metadata
   * @since 0.2.26
   */
  public abstract Backup getBackup(String backupId);

  /**
   * Gets the current status of a specific backup.
   *
   * @param backupId the unique identifier of the backup
   * @return the current BackupStatus of the backup
   * @since 0.2.26
   */
  public abstract BackupStatus getBackupStatus(String backupId);

  /**
   * Restores a collection/table from a backup.
   *
   * @param request the restore request containing restore configuration
   * @since 0.2.26
   */
  public abstract void restoreBackup(RestoreRequest request);

  /**
   * Deletes a specific backup.
   * WARNING: This operation is irreversible.
   *
   * @param backupId the unique identifier of the backup to delete
   * @since 0.2.26
   */
  public abstract void deleteBackup(String backupId);

  /**
   * Checks if this client has been closed.
   *
   * @return true if the client has been closed, false otherwise
   */
  protected boolean isClosed() {
    return closed;
  }

  /**
   * Ensures that the client is not closed before executing operations.
   *
   * @throws IllegalStateException if the client has been closed
   */
  protected void checkClosed() {
    if (closed) {
      throw new IllegalStateException("DBBackRestore client has been closed");
    }
  }

  /**
   * Builder base class for AbstractDBBackRestore implementations.
   *
   * @param <B> the builder type
   * @param <T> the type being built
   */
  public abstract static class Builder<T extends AbstractDBBackRestore, B extends Builder<T, B>>
      implements Provider.Builder {
    @Getter
    protected String region;
    @Getter
    protected String collectionName;
    protected String providerId;

    /**
     * Sets the region.
     *
     * @param region the region
     * @return this builder
     */
    public B withRegion(String region) {
      this.region = region;
      return self();
    }

    /**
     * Sets the collection/table name.
     *
     * @param collectionName the collection/table name
     * @return this builder
     */
    public B withCollectionName(String collectionName) {
      this.collectionName = collectionName;
      return self();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public B providerId(String providerId) {
      this.providerId = providerId;
      return self();
    }

    /**
     * Returns this builder instance.
     *
     * @return this builder
     */
    protected abstract B self();

    /**
     * Builds the DBBackRestore instance.
     *
     * @return the built instance
     */
    public abstract T build();
  }
}
