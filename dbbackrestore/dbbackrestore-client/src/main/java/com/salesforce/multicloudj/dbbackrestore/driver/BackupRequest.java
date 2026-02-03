package com.salesforce.multicloudj.dbbackrestore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Request object for creating a backup of a database table.
 *
 * @since 0.2.25
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BackupRequest {

    /**
     * Name to assign to the backup.
     * This should be unique within the scope of the table.
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
}
