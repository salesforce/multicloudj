package com.salesforce.multicloudj.dbbackrestore.driver;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the BackupRequest model class.
 */
public class BackupRequestTest {

    @Test
    void testBackupRequestBuilder() {
        Instant expiry = Instant.now().plusSeconds(86400);
        Map<String, String> options = new HashMap<>();
        options.put("encryption", "AES256");
        options.put("compression", "gzip");

        BackupRequest request = BackupRequest.builder()
                .backupName("my-backup")
                .description("Test backup")
                .expiryTime(expiry)
                .options(options)
                .build();

        assertEquals("my-backup", request.getBackupName());
        assertEquals("Test backup", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
        assertEquals(2, request.getOptions().size());
        assertEquals("AES256", request.getOptions().get("encryption"));
        assertEquals("gzip", request.getOptions().get("compression"));
    }

    @Test
    void testBackupRequestSetters() {
        BackupRequest request = new BackupRequest();
        Instant expiry = Instant.now().plusSeconds(3600);

        request.setBackupName("updated-backup");
        request.setDescription("Updated description");
        request.setExpiryTime(expiry);
        request.setOptions(Map.of("key", "value"));

        assertEquals("updated-backup", request.getBackupName());
        assertEquals("Updated description", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
        assertEquals(1, request.getOptions().size());
    }

    @Test
    void testBackupRequestAllArgsConstructor() {
        Instant expiry = Instant.now().plusSeconds(7200);
        Map<String, String> options = Map.of("type", "full");

        BackupRequest request = new BackupRequest("backup-all-args", "Description", expiry, options);

        assertEquals("backup-all-args", request.getBackupName());
        assertEquals("Description", request.getDescription());
        assertEquals(expiry, request.getExpiryTime());
        assertEquals(options, request.getOptions());
    }
}
