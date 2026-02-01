package com.salesforce.multicloudj.dbbackrestore.driver;

/**
 * Represents the status of a backup operation.
 *
 * @since 0.2.25
 */
public enum BackupStatus {
  /**
   * Backup is being created.
   */
  CREATING,

  /**
   * Backup is available and can be used for restoration.
   */
  AVAILABLE,

  /**
   * Backup is being deleted.
   */
  DELETING,

  /**
   * Backup has been deleted.
   */
  DELETED,

  /**
   * Backup creation or operation failed.
   */
  FAILED,

  /**
   * Backup status is unknown.
   */
  UNKNOWN
}
