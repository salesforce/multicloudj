package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for ObjectLockMode enum.
 */
class ObjectLockModeTest {

    @Test
    void testEnumValues() {
        // Verify all enum values exist
        ObjectLockMode[] values = ObjectLockMode.values();
        assertEquals(2, values.length);
        
        assertNotNull(ObjectLockMode.GOVERNANCE);
        assertNotNull(ObjectLockMode.COMPLIANCE);
    }

    @Test
    void testValueOf() {
        // Test valueOf for GOVERNANCE
        assertEquals(ObjectLockMode.GOVERNANCE, ObjectLockMode.valueOf("GOVERNANCE"));
        
        // Test valueOf for COMPLIANCE
        assertEquals(ObjectLockMode.COMPLIANCE, ObjectLockMode.valueOf("COMPLIANCE"));
    }

    @Test
    void testEnumOrdinal() {
        // Verify ordinal positions
        assertEquals(0, ObjectLockMode.GOVERNANCE.ordinal());
        assertEquals(1, ObjectLockMode.COMPLIANCE.ordinal());
    }

    @Test
    void testEnumName() {
        // Verify enum names
        assertEquals("GOVERNANCE", ObjectLockMode.GOVERNANCE.name());
        assertEquals("COMPLIANCE", ObjectLockMode.COMPLIANCE.name());
    }

    @Test
    void testEnumToString() {
        // Verify toString returns the enum name
        assertEquals("GOVERNANCE", ObjectLockMode.GOVERNANCE.toString());
        assertEquals("COMPLIANCE", ObjectLockMode.COMPLIANCE.toString());
    }
}
