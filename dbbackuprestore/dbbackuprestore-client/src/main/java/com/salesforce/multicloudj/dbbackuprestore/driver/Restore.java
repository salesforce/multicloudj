package com.salesforce.multicloudj.dbbackuprestore.driver;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a database restore operation.
 * Contains metadata about the restore including its status, source backup, and timing information.
 *
 * @since 0.2.25
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Restore {

    /**
     * Unique identifier for the restore operation.
     * This ID can be used to track the progress of the restore.
     */
    private String restoreId;

    /**
     * ID of the backup being restored from.
     */
    private String backupId;

    /**
     * Name of the target resource (table/database) being restored to.
     */
    private String targetResource;

    /**
     * Current status of the restore operation.
     */
    private RestoreStatus status;

    /**
     * Timestamp when the restore operation started.
     */
    private Instant startTime;

    /**
     * Timestamp when the restore operation completed.
     * Will be null if the restore is still in progress.
     */
    private Instant endTime;

    /**
     * Status or failure message from the restore job.
     * For failed jobs, this typically contains the provider-specific restore error reason.
     */
    private String statusMessage;
}
