package com.salesforce.multicloudj.dbbackrestore.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import java.util.List;
import lombok.Getter;

/**
 * Base class for database backup and restore implementations.
 * This abstract class provides the contract for backup and restore operations
 * across different cloud providers.
 *
 * @since 0.2.25
 */
public abstract class AbstractDBBackRestore implements Provider, AutoCloseable {

  private final String providerId;
  @Getter
  protected final String region;
  @Getter
  protected final String collectionName;

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
   * @param providerId the cloud provider identifier
   * @param region the region where the database is located
   * @param collectionName the name of the table to backup/restore
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
   * Lists all available backups for the table.
   *
   * @return a list of Backup objects representing available backups
   * @since 0.2.25
   */
  public abstract List<Backup> listBackups();

  /**
   * Gets details of a specific backup by its ID.
   *
   * @param backupId the unique identifier of the backup
   * @return the Backup object with full metadata
   * @since 0.2.25
   */
  public abstract Backup getBackup(String backupId);

  /**
   * Gets the current status of a specific backup.
   *
   * @param backupId the unique identifier of the backup
   * @return the current BackupStatus of the backup
   * @since 0.2.25
   */
  public abstract BackupStatus getBackupStatus(String backupId);

  /**
   * Restores a table from a backup.
   *
   * @param request the restore request containing restore configuration
   * @since 0.2.25
   */
  public abstract void restoreBackup(RestoreRequest request);

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
     * Sets the table name.
     *
     * @param collectionName the table name
     * @return this builder
     */
    public B withTableName(String collectionName) {
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
