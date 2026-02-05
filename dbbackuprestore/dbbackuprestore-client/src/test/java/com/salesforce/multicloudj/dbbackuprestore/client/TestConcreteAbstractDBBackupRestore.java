package com.salesforce.multicloudj.dbbackuprestore.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;

import java.util.List;

public class TestConcreteAbstractDBBackupRestore extends AbstractDBBackupRestore {

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
    public String restoreBackup(RestoreRequest request) {
        // No-op for testing
        return null;
    }

    @Override
    public Restore getRestoreJob(String restoreId) {
        return null;
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

    public static class Builder extends AbstractDBBackupRestore.Builder<TestConcreteAbstractDBBackupRestore, Builder> {
        public Builder() {
            providerId("mockProviderId");
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public TestConcreteAbstractDBBackupRestore build() {
            return new TestConcreteAbstractDBBackupRestore(this);
        }
    }
}
