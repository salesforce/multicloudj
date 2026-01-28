package com.salesforce.multicloudj.docstore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Request object for creating a backup of a document store collection/table.
 *
 * @since 0.2.26
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BackupRequest {

    /**
     * Name to assign to the backup.
     * This should be unique within the scope of the collection/table.
     */
    private String backupName;

    /**
     * Optional description for the backup.
     */
    private String description;

    /**
     * Optional expiry time for the backup.
     * If specified, the backup will be automatically deleted after this time.
     * Not all providers support expiry times.
     */
    private Instant expiryTime;

    /**
     * Provider-specific options as key-value pairs.
     * Can be used to pass additional cloud-specific parameters.
     */
    private java.util.Map<String, String> options;
}
