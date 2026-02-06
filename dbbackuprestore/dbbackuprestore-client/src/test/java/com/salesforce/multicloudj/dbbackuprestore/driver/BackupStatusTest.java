package com.salesforce.multicloudj.dbbackuprestore.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the BackupStatus enum.
 */
public class BackupStatusTest {

    @Test
    void testBackupStatusValues() {
        BackupStatus[] values = BackupStatus.values();
        assertEquals(6, values.length);

        assertTrue(contains(values, BackupStatus.CREATING));
        assertTrue(contains(values, BackupStatus.AVAILABLE));
        assertTrue(contains(values, BackupStatus.DELETING));
        assertTrue(contains(values, BackupStatus.DELETED));
        assertTrue(contains(values, BackupStatus.FAILED));
        assertTrue(contains(values, BackupStatus.UNKNOWN));

        assertEquals(BackupStatus.CREATING, BackupStatus.valueOf("CREATING"));
        assertEquals(BackupStatus.AVAILABLE, BackupStatus.valueOf("AVAILABLE"));
        assertEquals(BackupStatus.DELETING, BackupStatus.valueOf("DELETING"));
        assertEquals(BackupStatus.DELETED, BackupStatus.valueOf("DELETED"));
        assertEquals(BackupStatus.FAILED, BackupStatus.valueOf("FAILED"));
        assertEquals(BackupStatus.UNKNOWN, BackupStatus.valueOf("UNKNOWN"));
    }

    private boolean contains(BackupStatus[] values, BackupStatus status) {
        for (BackupStatus value : values) {
            if (value == status) {
                return true;
            }
        }
        return false;
    }
}
