package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;

public class TestBlobClient extends AbstractBlobClient<TestBlobClient> {
    public TestBlobClient(Builder builder) {
        super(builder);
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    protected ListBucketsResponse doListBuckets() {
        return null;
    }

    @Override
    protected void doCreateBucket(String bucketName) {
        // Test implementation - no-op
    }

    @Override
    public void close() {
        // Test implementation - no-op
    }

    public static class Builder extends AbstractBlobClient.Builder<TestBlobClient> {

        protected Builder() {
            providerId("test");
        }

        @Override
        public TestBlobClient build() {
            return new TestBlobClient(this);
        }
    }
}
