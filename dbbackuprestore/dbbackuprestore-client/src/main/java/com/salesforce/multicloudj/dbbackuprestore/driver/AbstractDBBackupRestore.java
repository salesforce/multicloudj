package com.salesforce.multicloudj.dbbackuprestore.driver;

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
public abstract class AbstractDBBackupRestore implements Provider, AutoCloseable {

  private final String providerId;
  @Getter
  protected final String region;
  @Getter
  protected final String resourceName;

  /**
   * Constructs an AbstractDBBackupRestore instance using a Builder.
   *
   * @param builder the Builder instance to use for construction
   */
  protected AbstractDBBackupRestore(Builder<?, ?> builder) {
    this(builder.providerId, builder.region, builder.resourceName);
  }

  /**
   * Constructs an AbstractDBBackupRestore instance.
   *
   * @param providerId the cloud provider identifier
   * @param region the region where the database is located
   * @param resourceName the name of the table to backup/restore
   */
  protected AbstractDBBackupRestore(String providerId, String region, String resourceName) {
    this.providerId = providerId;
    this.region = region;
    this.resourceName = resourceName;
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
   * Restores a table from a backup.
   *
   * @param request the restore request containing restore configuration
   * @return the unique identifier of the restore operation for status tracking
   * @since 0.2.25
   */
  public abstract String restoreBackup(RestoreRequest request);

  /**
   * Gets details of a specific restore operation by its ID.
   *
   * @param restoreId the unique identifier of the restore operation
   * @return the Restore object with full metadata
   * @since 0.2.25
   */
  public abstract Restore getRestoreJob(String restoreId);

  /**
   * Builder base class for AbstractDBBackupRestore implementations.
   *
   * @param <B> the builder type
   * @param <T> the type being built
   */
  public abstract static class Builder<T extends AbstractDBBackupRestore, B extends Builder<T, B>>
      implements Provider.Builder {
    @Getter
    protected String region;
    @Getter
    protected String resourceName;
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
     * @param resourceName the table name
     * @return this builder
     */
    public B withResourceName(String resourceName) {
      this.resourceName = resourceName;
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
     * Builds the DBBackupRestore instance.
     *
     * @return the built instance
     */
    public abstract T build();
  }
}
