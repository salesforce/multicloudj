package com.salesforce.multicloudj.dbbackuprestore.driver;

/**
 * Represents the status of a restore operation.
 *
 * @since 0.2.25
 */
public enum RestoreStatus {
    /**
     * Restore operation is in progress.
     */
    RESTORING,

    /**
     * Restore operation completed successfully.
     */
    COMPLETED,

    /**
     * Restore operation failed.
     */
    FAILED,

    /**
     * Restore status is unknown.
     */
    UNKNOWN
}
