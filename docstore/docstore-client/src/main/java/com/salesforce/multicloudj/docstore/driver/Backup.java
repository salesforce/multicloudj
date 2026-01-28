package com.salesforce.multicloudj.docstore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Represents a backup of a document store collection/table.
 * Contains metadata about the backup including its identifier, status, timestamps, and size.
 *
 * @since 0.2.26
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Backup {

    /**
     * Unique identifier for the backup.
     * This is provider-specific (e.g., AWS backup ARN, GCP backup name).
     */
    private String backupId;

    /**
     * Name of the collection/table that this backup represents.
     */
    private String collectionName;

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
     * Description or metadata about the backup.
     */
    private String description;

    /**
     * Provider-specific metadata as key-value pairs.
     * Can be used to store additional cloud-specific information.
     */
    private java.util.Map<String, String> metadata;
}
