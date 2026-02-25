package com.salesforce.multicloudj.dbbackuprestore.driver;

import com.salesforce.multicloudj.dbbackuprestore.client.TestConcreteAbstractDBBackupRestore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for AbstractDBBackupRestore base class.
 */
class AbstractDBBackupRestoreTest {

    /**
     * Test the constructor that takes a Builder.
     */
    @Test
    void testConstructorWithBuilder() {
        TestConcreteAbstractDBBackupRestore.Builder builder = new TestConcreteAbstractDBBackupRestore.Builder()
                .withRegion("us-west-2")
                .withResourceName("test-table");
        TestConcreteAbstractDBBackupRestore instance = builder.build();

        assertNotNull(instance);
        assertEquals("mockProviderId", instance.getProviderId());
        assertEquals("us-west-2", instance.getRegion());
        assertEquals("test-table", instance.getResourceName());
    }
}
