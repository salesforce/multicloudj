package com.salesforce.multicloudj.dbbackuprestore.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for AbstractDBBackupRestore base class.
 */
class AbstractDBBackupRestoreTest {

    /**
     * Test the constructor that takes a Builder.
     */
    @Test
    void testConstructorWithBuilder() {
        TestDBBackupRestore.Builder builder = new TestDBBackupRestore.Builder()
                .withRegion("us-west-2")
                .withResourceName("test-table");
        builder.providerId("test-provider");
        TestDBBackupRestore instance = builder.build();

        assertNotNull(instance);
        assertEquals("test-provider", instance.getProviderId());
        assertEquals("us-west-2", instance.getRegion());
        assertEquals("test-table", instance.getResourceName());
    }

    /**
     * Test the direct constructor.
     */
    @Test
    void testDirectConstructor() {
        TestDBBackupRestore instance = new TestDBBackupRestore(
                "direct-provider",
                "eu-west-1",
                "direct-table");

        assertEquals("direct-provider", instance.getProviderId());
        assertEquals("eu-west-1", instance.getRegion());
        assertEquals("direct-table", instance.getResourceName());
    }

    /**
     * Test builder providerId method.
     */
    @Test
    void testBuilderProviderId() {
        TestDBBackupRestore.Builder builder = new TestDBBackupRestore.Builder();
        TestDBBackupRestore.Builder result = builder.providerId("custom-provider");
        assertSame(builder, result);
    }

    /**
     * Concrete test implementation of AbstractDBBackupRestore for testing.
     */
    private static class TestDBBackupRestore extends AbstractDBBackupRestore {

        public TestDBBackupRestore(Builder builder) {
            super(builder);
        }

        public TestDBBackupRestore(String providerId, String region, String resourceName) {
            super(providerId, region, resourceName);
        }

        @Override
        public List<Backup> listBackups() {
            return Collections.emptyList();
        }

        @Override
        public Backup getBackup(String backupId) {
            return Backup.builder()
                    .backupId(backupId)
                    .resourceName(getResourceName())
                    .status(BackupStatus.AVAILABLE)
                    .build();
        }

        @Override
        public BackupStatus getBackupStatus(String backupId) {
            return BackupStatus.AVAILABLE;
        }

        @Override
        public void restoreBackup(RestoreRequest request) {
            // No-op for testing
        }

        @Override
        public void close() {
            // No-op for testing
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return UnknownException.class;
        }

        @Override
        public Builder builder() {
            return new Builder();
        }

        /**
         * Test builder implementation.
         */
        public static class Builder
                extends AbstractDBBackupRestore.Builder<TestDBBackupRestore, Builder> {

            public Builder() {
                this.providerId = "test-provider";
            }

            @Override
            protected Builder self() {
                return this;
            }

            @Override
            public TestDBBackupRestore build() {
                return new TestDBBackupRestore(this);
            }
        }
    }
}
