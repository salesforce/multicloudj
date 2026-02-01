package com.salesforce.multicloudj.dbbackrestore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Represents a backup of a database table.
 * Contains metadata about the backup including its identifier, status, timestamps, and size.
 *
 * @since 0.2.25
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Backup {

    /**
     * Unique identifier for the backup.
     * Format and content are provider-specific (e.g., ARN for AWS, snapshot ID for Alibaba).
     */
    private String backupId;

    /**
     * Name of the table that this backup represents.
     */
    private String resourceName;

    /**
     * Current status of the backup.
     */
    private BackupStatus status;

    /**
     * Timestamp when the backup was created.
     */
    private Instant creationTime;

    /**
     * Timestamp when the backup expires (if applicable).
     * Null if the backup does not have an expiration.
     */
    private Instant expiryTime;

    /**
     * Size of the backup in bytes (if available).
     * -1 if size information is not available.
     */
    private long sizeInBytes;

    /**
     * Description or human-readable name for the backup.
     */
    private String description;

    /**
     * Vault identifier where the backup is stored.
     */
    private String vaultId;
}
