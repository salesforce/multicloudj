package com.salesforce.multicloudj.blob.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for RetentionMode enum.
 */
class RetentionModeTest {

    @Test
    void testEnumValues() {
        // Verify all enum values exist
        RetentionMode[] values = RetentionMode.values();
        assertEquals(2, values.length);
        
        assertNotNull(RetentionMode.GOVERNANCE);
        assertNotNull(RetentionMode.COMPLIANCE);
    }

    @Test
    void testValueOf() {
        // Test valueOf for GOVERNANCE
        assertEquals(RetentionMode.GOVERNANCE, RetentionMode.valueOf("GOVERNANCE"));
        
        // Test valueOf for COMPLIANCE
        assertEquals(RetentionMode.COMPLIANCE, RetentionMode.valueOf("COMPLIANCE"));
    }

    @Test
    void testEnumOrdinal() {
        // Verify ordinal positions
        assertEquals(0, RetentionMode.GOVERNANCE.ordinal());
        assertEquals(1, RetentionMode.COMPLIANCE.ordinal());
    }

    @Test
    void testEnumName() {
        // Verify enum names
        assertEquals("GOVERNANCE", RetentionMode.GOVERNANCE.name());
        assertEquals("COMPLIANCE", RetentionMode.COMPLIANCE.name());
    }

    @Test
    void testEnumToString() {
        // Verify toString returns the enum name
        assertEquals("GOVERNANCE", RetentionMode.GOVERNANCE.toString());
        assertEquals("COMPLIANCE", RetentionMode.COMPLIANCE.toString());
    }
}
