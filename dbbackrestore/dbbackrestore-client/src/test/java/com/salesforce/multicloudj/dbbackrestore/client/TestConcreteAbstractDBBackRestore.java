package com.salesforce.multicloudj.dbbackrestore.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;

import java.util.List;

public class TestConcreteAbstractDBBackRestore extends AbstractDBBackRestore {

    public TestConcreteAbstractDBBackRestore(TestConcreteAbstractDBBackRestore.Builder builder) {
        super(builder);
    }

    public TestConcreteAbstractDBBackRestore() {
        super(new Builder());
    }

    @Override
    public List<Backup> listBackups() {
        return null;
    }

    @Override
    public Backup getBackup(String backupId) {
        return null;
    }

    @Override
    public BackupStatus getBackupStatus(String backupId) {
        return null;
    }

    @Override
    public void restoreBackup(RestoreRequest request) {
        // No-op for testing
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public void close() throws Exception {
        // No-op for testing
    }

    public static class Builder extends AbstractDBBackRestore.Builder<TestConcreteAbstractDBBackRestore, Builder> {
        protected Builder() {
            providerId("mockProviderId");
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public TestConcreteAbstractDBBackRestore build() {
            return new TestConcreteAbstractDBBackRestore(this);
        }
    }
}
