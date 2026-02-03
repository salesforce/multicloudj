package com.salesforce.multicloudj.dbbackuprestore.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;

import java.util.List;

public class TestConcreteAbstractDBBackupRestore extends com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore {

    public TestConcreteAbstractDBBackupRestore(TestConcreteAbstractDBBackupRestore.Builder builder) {
        super(builder);
    }

    public TestConcreteAbstractDBBackupRestore() {
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

    public static class Builder extends com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore.Builder<TestConcreteAbstractDBBackupRestore, Builder> {
        protected Builder() {
            providerId("mockProviderId");
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public TestConcreteAbstractDBBackupRestore build() {
            return new TestConcreteAbstractDBBackupRestore(this);
        }
    }
}
