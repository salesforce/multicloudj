package com.salesforce.multicloudj.dbbackrestore.driver;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the Backup model class.
 */
public class BackupTest {

    @Test
    void testBackupBuilder() {
        Instant now = Instant.now();
        Instant expiry = now.plusSeconds(86400);

        Backup backup = Backup.builder()
                .backupId("backup-123")
                .resourceName("test-table")
                .status(BackupStatus.AVAILABLE)
                .creationTime(now)
                .expiryTime(expiry)
                .sizeInBytes(1024L)
                .description("Test backup")
                .vaultId("vault-abc")
                .build();

        assertEquals("backup-123", backup.getBackupId());
        assertEquals("test-table", backup.getResourceName());
        assertEquals(BackupStatus.AVAILABLE, backup.getStatus());
        assertEquals(now, backup.getCreationTime());
        assertEquals(expiry, backup.getExpiryTime());
        assertEquals(1024L, backup.getSizeInBytes());
        assertEquals("Test backup", backup.getDescription());
        assertEquals("vault-abc", backup.getVaultId());
    }
}
