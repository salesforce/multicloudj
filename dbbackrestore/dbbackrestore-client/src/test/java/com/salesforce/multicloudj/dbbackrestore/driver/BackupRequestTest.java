package com.salesforce.multicloudj.dbbackrestore.driver;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the BackupRequest model class.
 */
public class BackupRequestTest {

    @Test
    void testBackupRequestBuilder() {
        Instant expiry = Instant.now().plusSeconds(86400);

        BackupRequest request = BackupRequest.builder()
                .backupName("my-backup")
                .description("Test backup")
                .expiryTime(expiry)
                .build();

        assertEquals("my-backup", request.getBackupName());
        assertEquals("Test backup", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
    }

    @Test
    void testBackupRequestSetters() {
        BackupRequest request = new BackupRequest();
        Instant expiry = Instant.now().plusSeconds(3600);

        request.setBackupName("updated-backup");
        request.setDescription("Updated description");
        request.setExpiryTime(expiry);

        assertEquals("updated-backup", request.getBackupName());
        assertEquals("Updated description", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
    }

    @Test
    void testBackupRequestAllArgsConstructor() {
        Instant expiry = Instant.now().plusSeconds(7200);

        BackupRequest request = new BackupRequest("backup-all-args", "Description", expiry);

        assertEquals("backup-all-args", request.getBackupName());
        assertEquals("Description", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
    }
}
